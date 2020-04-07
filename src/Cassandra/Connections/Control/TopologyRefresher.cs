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

namespace Cassandra.Connections.Control
{
    /// <inheritdoc />
    internal class TopologyRefresher : ITopologyRefresher
    {
        private const string SelectPeers = "SELECT * FROM system.peers";
        private const string SelectLocal = "SELECT * FROM system.local WHERE key='local'";

        private static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);

        private readonly Configuration _config;
        private readonly Metadata _metadata;

        public TopologyRefresher(Metadata metadata, Configuration config)
        {
            _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <inheritdoc />
        public async Task<Host> RefreshNodeListAsync(IConnectionEndPoint currentEndPoint, IConnection connection, ProtocolVersion version)
        {
            ControlConnection.Logger.Info("Refreshing node list");

            var queriesRs = await Task.WhenAll(
                                          _config.MetadataRequestHandler.SendMetadataRequestAsync(
                                              connection, version, TopologyRefresher.SelectLocal, QueryProtocolOptions.Default), 
                                          _config.MetadataRequestHandler.SendMetadataRequestAsync(
                                              connection, version, TopologyRefresher.SelectPeers, QueryProtocolOptions.Default))
                                      .ConfigureAwait(false);

            var localRow = _config.MetadataRequestHandler.GetRowSet(queriesRs[0]).FirstOrDefault();
            var rsPeers = _config.MetadataRequestHandler.GetRowSet(queriesRs[1]);

            if (localRow == null)
            {
                ControlConnection.Logger.Error("Local host metadata could not be retrieved");
                throw new DriverInternalError("Local host metadata could not be retrieved");
            }

            _metadata.Partitioner = localRow.GetValue<string>("partitioner");
            var host = GetAndUpdateLocalHost(currentEndPoint, localRow);
            UpdatePeersInfo(rsPeers, host);
            ControlConnection.Logger.Info("Node list retrieved successfully");
            return host;
        }

        /// <summary>
        /// Parses system.local response, creates the local Host and adds it to the Hosts collection.
        /// </summary>
        private Host GetAndUpdateLocalHost(IConnectionEndPoint endPoint, IRow row)
        {
            var hostIpEndPoint = endPoint.GetOrParseHostIpEndPoint(row, _config.AddressTranslator, _config.ProtocolOptions.Port);
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
        /// <param name="rs"></param>
        /// <param name="currentHost"></param>
        private void UpdatePeersInfo(IEnumerable<IRow> rs, Host currentHost)
        {
            var foundPeers = new HashSet<IPEndPoint>();
            foreach (var row in rs)
            {
                var address = TopologyRefresher.GetAddressForLocalOrPeerHost(row, _config.AddressTranslator, _config.ProtocolOptions.Port);
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
        internal static IPEndPoint GetAddressForLocalOrPeerHost(IRow row, IAddressTranslator translator, int port)
        {
            var address = row.GetValue<IPAddress>("rpc_address");
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

            return translator.Translate(new IPEndPoint(address, port));
        }
    }
}