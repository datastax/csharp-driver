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
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections.Control;

namespace Cassandra.Connections
{
    internal class HostnameContactPoint : IContactPoint
    {
        private readonly IDnsResolver _dns;
        private readonly ProtocolOptions _protocolOptions;
        private readonly IServerNameResolver _serverNameResolver;
        private readonly string _hostname;
        private volatile IEnumerable<IConnectionEndPoint> _cachedEndpoints = new List<IConnectionEndPoint>();
        private readonly bool _keepContactPointsUnresolved;

        public HostnameContactPoint(
            IDnsResolver dnsResolver, 
            ProtocolOptions protocolOptions, 
            IServerNameResolver serverNameResolver, 
            bool keepContactPointsUnresolved, 
            string hostname)
        {
            _dns = dnsResolver ?? throw new ArgumentNullException(nameof(dnsResolver));
            _protocolOptions = protocolOptions ?? throw new ArgumentNullException(nameof(protocolOptions));
            _serverNameResolver = serverNameResolver ?? throw new ArgumentNullException(nameof(serverNameResolver));
            _keepContactPointsUnresolved = keepContactPointsUnresolved;
            _hostname = hostname ?? throw new ArgumentNullException(nameof(hostname));
        }

        public bool CanBeResolved => true;

        public string StringRepresentation => _hostname;
        
        public override string ToString()
        {
            return StringRepresentation;
        }

        public Task<IEnumerable<IConnectionEndPoint>> GetConnectionEndPointsAsync(bool refreshCache)
        {
            return ResolveContactPointAsync(refreshCache);
        }

        /// <summary>
        /// Resolves the contact point according to its type. If it is not an IP address, then considers it a hostname and
        /// attempts to resolve it with DNS.
        /// </summary>
        private async Task<IEnumerable<IConnectionEndPoint>> ResolveContactPointAsync(bool refreshCache)
        {
            if (!refreshCache && !_keepContactPointsUnresolved && _cachedEndpoints.Any())
            {
                return _cachedEndpoints;
            }

            IPHostEntry hostEntry = null;
            try
            {
                hostEntry = await _dns.GetHostEntryAsync(_hostname).ConfigureAwait(false);
            }
            catch (Exception)
            {
                Cluster.Logger.Warning("Contact point '{0}' could not be resolved.", _hostname);
            }

            var connectionEndPoints = new List<IConnectionEndPoint>();

            if (hostEntry != null && hostEntry.AddressList.Length > 0)
            {
                if (hostEntry.AddressList.Length > 1)
                {
                    Cluster.Logger.Info("Contact point '{0}' resolved to multiple ({1}) addresses. Will attempt to use them all if necessary: '{2}'",
                        _hostname,
                        hostEntry.AddressList.Length,
                        string.Join(",", hostEntry.AddressList.Select(resolvedAddress => resolvedAddress.ToString())));
                }

                connectionEndPoints.AddRange(
                    hostEntry.AddressList.Select(resolvedAddress =>
                        new ConnectionEndPoint(new IPEndPoint(resolvedAddress, _protocolOptions.Port), _serverNameResolver, this)));
            }

            _cachedEndpoints = connectionEndPoints;
            return _cachedEndpoints;
        }

        private bool TypedEquals(HostnameContactPoint other)
        {
            return Equals(_hostname, other._hostname);
        }

        public bool Equals(IContactPoint other)
        {
            return Equals((object)other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is HostnameContactPoint typedObj)
            {
                return TypedEquals(typedObj);
            }

            return false;
        }

        public override int GetHashCode()
        {
            return _hostname.GetHashCode();
        }
    }
}