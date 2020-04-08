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

namespace Dse.Connections
{
    /// <inheritdoc />
    internal class ConnectionEndPoint : IConnectionEndPoint
    {
        private readonly Func<IPEndPoint, string> _serverNameResolver;

        public ConnectionEndPoint(IPEndPoint hostIpEndPoint, Func<IPEndPoint, string> serverNameResolver)
        {
            _serverNameResolver = serverNameResolver;
            SocketIpEndPoint = hostIpEndPoint;
            EndpointFriendlyName = hostIpEndPoint.ToString();
        }

        /// <inheritdoc />
        public IPEndPoint SocketIpEndPoint { get; }
        
        /// <inheritdoc />
        public string EndpointFriendlyName { get; }

        /// <inheritdoc />
        public Task<string> GetServerNameAsync()
        {
            return Task.Factory.StartNew(() => _serverNameResolver.Invoke(SocketIpEndPoint));
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return obj is ConnectionEndPoint endpoint &&
                   SocketIpEndPoint.Equals(endpoint.SocketIpEndPoint);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return SocketIpEndPoint.GetHashCode();
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
        public IPEndPoint GetOrParseHostIpEndPoint(IRow row, IAddressTranslator translator, int port)
        {
            return SocketIpEndPoint;
        }
    }
}