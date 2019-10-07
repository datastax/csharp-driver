//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using Dse.Tasks;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Threading.Tasks;

namespace Dse.Connections
{
    internal class EndPointResolver : BaseEndPointResolver
    {
        private readonly IDnsResolver _dns;
        private readonly ProtocolOptions _protocolOptions;

        private readonly ConcurrentDictionary<string, IReadOnlyList<IConnectionEndPoint>> _resolvedContactPoints =
            new ConcurrentDictionary<string, IReadOnlyList<IConnectionEndPoint>>();

        public EndPointResolver(IDnsResolver dns, ProtocolOptions protocolOptions)
        {
            _dns = dns;
            _protocolOptions = protocolOptions;
        }

        /// <inheritdoc />
        public override Task<IConnectionEndPoint> GetConnectionEndPointAsync(Host host, bool refreshCache)
        {
            return Task.FromResult((IConnectionEndPoint)new ConnectionEndPoint(host.Address, GetServerName));
        }

        /// <inheritdoc />
        public override Task<IEnumerable<IConnectionEndPoint>> GetOrResolveContactPointAsync(object contactPoint)
        {
            if (contactPoint is IPEndPoint endpoint)
            {
                var connectionEndPoint = new ConnectionEndPoint(endpoint, GetServerName);
                var listOfConnectionEndPoints = new List<IConnectionEndPoint> { connectionEndPoint };
                return Task.FromResult(listOfConnectionEndPoints.AsEnumerable());
            }

            if (!(contactPoint is string contactPointText))
            {
                throw new InvalidOperationException("Contact points should be either string or IPEndPoint instances");
            }

            return ResolveContactPointAsync(contactPointText);
        }

        /// <inheritdoc />
        public override Task RefreshContactPointCache()
        {
            _resolvedContactPoints.Clear();
            return TaskHelper.Completed;
        }

        /// <summary>
        /// Gets the host name to be used in <see cref="SslStream.AuthenticateAsClientAsync(string)"/> when SSL is enabled.
        /// </summary>
        private string GetServerName(IPEndPoint socketIpEndPoint)
        {
            try
            {
                return _protocolOptions.SslOptions.HostNameResolver(socketIpEndPoint.Address);
            }
            catch (Exception ex)
            {
                TcpSocket.Logger.Error(
                    $"SSL connection: Can not resolve host name for address {socketIpEndPoint.Address}." +
                    " Using the IP address instead of the host name. This may cause RemoteCertificateNameMismatch " +
                    "error during Cassandra host authentication. Note that the Cassandra node SSL certificate's " +
                    "CN(Common Name) must match the Cassandra node hostname.", ex);
                return socketIpEndPoint.Address.ToString();
            }
        }

        /// <summary>
        /// Resolves the contact point according to its type. If it is not an IP address, then considers it a hostname and
        /// attempts to resolve it with DNS.
        /// </summary>
        private async Task<IEnumerable<IConnectionEndPoint>> ResolveContactPointAsync(string contactPointText)
        {
            if (_resolvedContactPoints.TryGetValue(contactPointText, out var ipEndPoints))
            {
                return ipEndPoints;
            }

            if (IPAddress.TryParse(contactPointText, out var ipAddress))
            {
                var ipEndpoint = new IPEndPoint(ipAddress, _protocolOptions.Port);
                var connectionEndPoint = new ConnectionEndPoint(ipEndpoint, GetServerName);
                var listOfConnectionEndPoints = new List<IConnectionEndPoint> { connectionEndPoint };
                _resolvedContactPoints.AddOrUpdate(
                    contactPointText,
                    key => listOfConnectionEndPoints,
                    (key, list) => listOfConnectionEndPoints);
                return listOfConnectionEndPoints;
            }

            IPHostEntry hostEntry = null;
            try
            {
                hostEntry = await _dns.GetHostEntryAsync(contactPointText).ConfigureAwait(false);
            }
            catch (Exception)
            {
                Cluster.Logger.Warning($"Host '{contactPointText}' could not be resolved");
            }

            var connectionEndPoints = new List<IConnectionEndPoint>();

            if (hostEntry != null && hostEntry.AddressList.Length > 0)
            {
                connectionEndPoints.AddRange(
                    hostEntry.AddressList.Select(resolvedAddress =>
                        new ConnectionEndPoint(new IPEndPoint(resolvedAddress, _protocolOptions.Port), GetServerName)));
            }

            _resolvedContactPoints.AddOrUpdate(
                contactPointText,
                key => connectionEndPoints,
                (key, list) => connectionEndPoints);

            return connectionEndPoints;
        }
    }
}