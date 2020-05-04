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
using System.Text;
using System.Threading.Tasks;
using Cassandra.Connections.Control;

namespace Cassandra.Connections
{
    /// <inheritdoc />
    internal class SniConnectionEndPoint : IConnectionEndPoint
    {
        private readonly string _serverName;
        private readonly IPEndPoint _hostIpEndPoint;

        public SniConnectionEndPoint(IPEndPoint socketIpEndPoint, string serverName, IContactPoint contactPoint) :
            this(socketIpEndPoint, null, serverName, contactPoint)
        {
        }

        public SniConnectionEndPoint(IPEndPoint socketIpEndPoint, IPEndPoint hostIpEndPoint, string serverName, IContactPoint contactPoint)
        {
            SocketIpEndPoint = socketIpEndPoint ?? throw new ArgumentNullException(nameof(socketIpEndPoint));
            _hostIpEndPoint = hostIpEndPoint;
            _serverName = serverName;
            ContactPoint = contactPoint;

            var stringBuilder = new StringBuilder(hostIpEndPoint?.ToString() ?? socketIpEndPoint.ToString());

            if (hostIpEndPoint == null && serverName != null)
            {
                stringBuilder.Append($" ({serverName})");
            }

            EndpointFriendlyName = stringBuilder.ToString();
        }

        /// <inheritdoc />
        public IContactPoint ContactPoint { get; }

        /// <inheritdoc />
        public IPEndPoint SocketIpEndPoint { get; }

        /// <inheritdoc />
        public string EndpointFriendlyName { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            return EndpointFriendlyName;
        }

        /// <inheritdoc />
        public Task<string> GetServerNameAsync()
        {
            return Task.FromResult(_serverName);
        }

        /// <inheritdoc />
        public IPEndPoint GetHostIpEndPointWithFallback()
        {
            return _hostIpEndPoint ?? SocketIpEndPoint;
        }

        /// <inheritdoc />
        public IPEndPoint GetHostIpEndPoint()
        {
            return _hostIpEndPoint;
        }

        public bool Equals(IConnectionEndPoint other)
        {
            return Equals((object)other);
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SniConnectionEndPoint point))
            {
                return false;
            }

            if (!object.Equals(_hostIpEndPoint, point._hostIpEndPoint))
            {
                return false;
            }

            if (!object.Equals(SocketIpEndPoint, point.SocketIpEndPoint))
            {
                return false;
            }

            return _serverName == point._serverName;
        }

        public override int GetHashCode()
        {
            return Utils.CombineHashCodeWithNulls(new object[]
            {
                _hostIpEndPoint, _serverName, SocketIpEndPoint
            });
        }
    }
}