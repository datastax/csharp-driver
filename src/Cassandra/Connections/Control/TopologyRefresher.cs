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
using Cassandra.Serialization;
using Cassandra.Tasks;

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
        public async Task<Host> RefreshNodeListAsync(
            IConnectionEndPoint currentEndPoint, IConnection connection, ISerializer serializer)
        {
            ControlConnection.Logger.Info("Refreshing node list");

            // safe guard against concurrent changes of this field
            var localIsPeersV2 = _isPeersV2;

            var localTask = SendSystemLocalRequestAsync(connection, serializer);
            var peersTask = SendSystemPeersRequestAsync(localIsPeersV2, connection, serializer);
            
            await Task.WhenAll(localTask, peersTask).ConfigureAwait(false);

            var peersResponse = peersTask.Result;
            localIsPeersV2 = peersResponse.IsPeersV2;

            var rsPeers = _config.MetadataRequestHandler.GetRowSet(peersResponse.Response);
            
            var localRow = _config.MetadataRequestHandler.GetRowSet(localTask.Result).FirstOrDefault();
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
        
        private Task<Response> SendSystemLocalRequestAsync(IConnection connection, ISerializer serializer)
        {
            return _config.MetadataRequestHandler.SendMetadataRequestAsync(
                connection, serializer, TopologyRefresher.SelectLocal, QueryProtocolOptions.Default);
        }

        private Task<PeersResponse> SendSystemPeersRequestAsync(bool isPeersV2, IConnection connection, ISerializer serializer)
        {
            var peersTask = _config.MetadataRequestHandler.SendMetadataRequestAsync(
                connection, 
                serializer, 
                isPeersV2 ? TopologyRefresher.SelectPeersV2 : TopologyRefresher.SelectPeers, 
                QueryProtocolOptions.Default);

            return GetPeersResponseAsync(isPeersV2, peersTask, connection, serializer);
        }

        /// <summary>
        /// Handles fallback logic when peers_v2 table is missing.
        /// </summary>
        private async Task<PeersResponse> GetPeersResponseAsync(
            bool isPeersV2, Task<Response> peersRequest, IConnection connection, ISerializer serializer)
        {
            if (!isPeersV2)
            {
                var peersResponse = await peersRequest.ConfigureAwait(false);
                return new PeersResponse { IsPeersV2 = false, Response = peersResponse };
            }

            try
            {
                var peersResponse = await peersRequest.ConfigureAwait(false);
                return new PeersResponse { IsPeersV2 = true, Response = peersResponse };
            }
            catch (InvalidQueryException)
            {
                ControlConnection.Logger.Verbose(
                    "Failed to retrieve data from system.peers_v2, falling back to system.peers for " +
                    "the remainder of this cluster instance's lifetime.");

                _isPeersV2 = false;

                peersRequest = _config.MetadataRequestHandler.SendMetadataRequestAsync(
                    connection, serializer, TopologyRefresher.SelectPeers, QueryProtocolOptions.Default);

                return await GetPeersResponseAsync(false, peersRequest, connection, serializer).ConfigureAwait(false);
            }
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

            if (TopologyRefresher.BindAllAddress.Equals(address))
            {
                if (row.ContainsColumn("peer") && !row.IsNull("peer"))
                {
                    // system.peers
                    address = row.GetValue<IPAddress>("peer");
                }
                else if (row.ContainsColumn("broadcast_address") && !row.IsNull("broadcast_address"))
                {
                    // system.local
                    address = row.GetValue<IPAddress>("broadcast_address");
                }
                else if (row.ContainsColumn("listen_address") && !row.IsNull("listen_address"))
                {
                    // system.local
                    address = row.GetValue<IPAddress>("listen_address");
                }
                else
                {
                    ControlConnection.Logger.Error(
                        "Found host with 0.0.0.0 as rpc_address and nulls as listen_address and broadcast_address. " +
                        "Because of this, the driver can not connect to this node.");
                    return null;
                }

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

        private class PeersResponse
        {
            public bool IsPeersV2 { get; set;  }

            public Response Response { get; set;  }
        }
    }
}