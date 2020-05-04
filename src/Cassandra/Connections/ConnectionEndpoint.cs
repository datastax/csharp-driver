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

using System;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections.Control;

namespace Cassandra.Connections
{
    /// <inheritdoc />
    internal class ConnectionEndPoint : IConnectionEndPoint
    {
        private readonly IServerNameResolver _serverNameResolver;
        
        public ConnectionEndPoint(IPEndPoint hostIpEndPoint, IServerNameResolver serverNameResolver, IContactPoint contactPoint)
        {
            _serverNameResolver = serverNameResolver ?? throw new ArgumentNullException(nameof(serverNameResolver));
            ContactPoint = contactPoint;
            SocketIpEndPoint = hostIpEndPoint ?? throw new ArgumentNullException(nameof(hostIpEndPoint));
            EndpointFriendlyName = hostIpEndPoint.ToString();
        }

        /// <inheritdoc />
        public IContactPoint ContactPoint { get; }

        /// <inheritdoc />
        public IPEndPoint SocketIpEndPoint { get; }

        /// <inheritdoc />
        public string EndpointFriendlyName { get; }

        /// <inheritdoc />
        public Task<string> GetServerNameAsync()
        {
            return Task.Factory.StartNew(() => _serverNameResolver.GetServerName(SocketIpEndPoint));
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return EndpointFriendlyName;
        }

        /// <inheritdoc />
        public IPEndPoint GetHostIpEndPointWithFallback()
        {
            return SocketIpEndPoint;
        }

        /// <inheritdoc />
        public IPEndPoint GetHostIpEndPoint()
        {
            return SocketIpEndPoint;
        }

        public bool Equals(IConnectionEndPoint other)
        {
            return Equals((object)other);
        }

        public override bool Equals(object obj)
        {
            if (!(obj is ConnectionEndPoint point))
            {
                return false;
            }

            return object.Equals(SocketIpEndPoint, point.SocketIpEndPoint);
        }

        public override int GetHashCode()
        {
            return SocketIpEndPoint.GetHashCode();
        }
    }
}