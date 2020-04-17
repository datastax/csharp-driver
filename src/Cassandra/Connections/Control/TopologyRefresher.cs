// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Responses;

namespace Cassandra.Connections.Control
{
    /// <inheritdoc />
    internal class TopologyRefresher : ITopologyRefresher
    {
        private const string SelectPeers = "SELECT * FROM system.peers";
        private const string SelectPeersV2 = "SELECT * FROM system.peers_v2";
        private const string SelectLocal = "SELECT * FROM system.local WHERE key='local'";

        private static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);

        private readonly Configuration _config;
        private readonly Metadata _metadata;

        /// <summary>
        /// Once this is set to false, it will never be set to true again.
        /// </summary>
        private volatile bool _isPeersV2 = true;

        public TopologyRefresher(Metadata metadata, Configuration config)
        {
            _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <inheritdoc />
        public async Task<Host> RefreshNodeListAsync(IConnectionEndPoint currentEndPoint, IConnection connection, ProtocolVersion version)
        {
            ControlConnection.Logger.Info("Refreshing node list");

            // safe guard against concurrent changes of this field
            var localIsPeersV2 = _isPeersV2;

            var localTask = _config.MetadataRequestHandler.SendMetadataRequestAsync(
                connection, version, TopologyRefresher.SelectLocal, QueryProtocolOptions.Default);
            var peersTask = _config.MetadataRequestHandler.SendMetadataRequestAsync(
                connection, version, localIsPeersV2 ? TopologyRefresher.SelectPeersV2 : TopologyRefresher.SelectPeers, QueryProtocolOptions.Default);

            // Wait for both tasks to complete before checking for a missing peers_v2 table.
            try
            {
                await Task.WhenAll(localTask, peersTask).ConfigureAwait(false);
            }
            catch
            {
                if (localTask.Exception != null || !localIsPeersV2)
                {
                    throw;
                }
            }
            
            Response peersResponse = null;
            if (localIsPeersV2)
            {
                try
                {
                    peersResponse = await peersTask.ConfigureAwait(false);
                }
                catch (InvalidQueryException)
                {
                    ControlConnection.Logger.Verbose(
                        "Failed to retrieve data from system.peers_v2, falling back to system.peers for " +
                        "the remainder of this cluster instance's lifetime.");
                    _isPeersV2 = false;
                    localIsPeersV2 = false;
                    peersResponse = await _config.MetadataRequestHandler.SendMetadataRequestAsync(
                        connection, version, TopologyRefresher.SelectPeers, QueryProtocolOptions.Default).ConfigureAwait(false);
                }
            }

            var rsPeers = _config.MetadataRequestHandler.GetRowSet(peersResponse);
            
            var localRow = _config.MetadataRequestHandler.GetRowSet(await localTask.ConfigureAwait(false)).FirstOrDefault();
            if (localRow == null)
            {
                ControlConnection.Logger.Error("Local host metadata could not be retrieved");
                throw new DriverInternalError("Local host metadata could not be retrieved");
            }

            _metadata.Partitioner = localRow.GetValue<string>("partitioner");
            var host = GetAndUpdateLocalHost(currentEndPoint, localRow);
            UpdatePeersInfo(localIsPeersV2, rsPeers, host);
            ControlConnection.Logger.Info("Node list retrieved successfully");
            return host;
        }

        /// <summary>
        /// Parses system.local response, creates the local Host and adds it to the Hosts collection.
        /// </summary>
        private Host GetAndUpdateLocalHost(IConnectionEndPoint endPoint, IRow row)
        {
            var hostIpEndPoint = 
                endPoint.GetHostIpEndPoint() 
                ?? GetRpcEndPoint(false, row, _config.AddressTranslator, _config.ProtocolOptions.Port);

            if (hostIpEndPoint == null)
            {
                throw new DriverInternalError("Could not parse the node's ip address from system tables.");
            }

            var host = _metadata.GetHost(hostIpEndPoint) ?? _metadata.AddHost(hostIpEndPoint, endPoint.ContactPoint);

            // Update cluster name, DC and rack for the one node we are connected to
            var clusterName = row.GetValue<string>("cluster_name");

            if (clusterName != null)
            {
                _metadata.ClusterName = clusterName;
            }

            host.SetInfo(row);
            return host;
        }

        /// <summary>
        /// Parses response from system.peers and updates the hosts collection.
        /// </summary>
        private void UpdatePeersInfo(bool isPeersV2, IEnumerable<IRow> peersRs, Host currentHost)
        {
            var foundPeers = new HashSet<IPEndPoint>();
            foreach (var row in peersRs)
            {
                var address = GetRpcEndPoint(isPeersV2, row, _config.AddressTranslator, _config.ProtocolOptions.Port);
                if (address == null)
                {
                    ControlConnection.Logger.Error("No address found for host, ignoring it.");
                    continue;
                }

                foundPeers.Add(address);
                var host = _metadata.GetHost(address) ?? _metadata.AddHost(address);
                host.SetInfo(row);
            }

            // Removes all those that seems to have been removed (since we lost the control connection or not valid contact point)
            foreach (var address in _metadata.AllReplicas())
            {
                if (!address.Equals(currentHost.Address) && !foundPeers.Contains(address))
                {
                    _metadata.RemoveHost(address);
                }
            }
        }
        
        /// <summary>
        /// Parses address from system table query response and translates it using the provided <paramref name="translator"/>.
        /// </summary>
        internal IPEndPoint GetRpcEndPoint(bool isPeersV2, IRow row, IAddressTranslator translator, int defaultPort)
        {
            IPAddress address;
            address = isPeersV2 ? GetRpcAddressFromPeersV2(row) : GetRpcAddressFromLocalPeersV1(row);

            if (address == null)
            {
                return null;
            }

            if (TopologyRefresher.BindAllAddress.Equals(address) && !row.IsNull("peer"))
            {
                address = row.GetValue<IPAddress>("peer");
                ControlConnection.Logger.Warning(
                    "Found host with 0.0.0.0 as rpc_address, using listen_address ({0}) to contact it instead. " +
                    "If this is incorrect you should avoid the use of 0.0.0.0 server side.", address.ToString());
            }

            var rpcPort = defaultPort;
            if (isPeersV2)
            {
                var nullableRpcPort = GetRpcPortFromPeersV2(row);
                if (nullableRpcPort == null)
                {
                    ControlConnection.Logger.Warning(
                        "Found host with NULL native_port, using default port ({0}) to contact it instead. ", rpcPort);
                }
                else
                {
                    rpcPort = nullableRpcPort.Value;
                }
            }

            return translator.Translate(new IPEndPoint(address, rpcPort));
        }
        
        private IPAddress GetRpcAddressFromPeersV2(IRow row)
        {
            return row.GetValue<IPAddress>("native_address");
        }
        
        private IPAddress GetRpcAddressFromLocalPeersV1(IRow row)
        {
            return row.GetValue<IPAddress>("rpc_address");
        }
        
        private int? GetRpcPortFromPeersV2(IRow row)
        {
            return row.GetValue<int?>("native_port");
        }
    }
}