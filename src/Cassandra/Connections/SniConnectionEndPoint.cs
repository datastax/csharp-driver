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

using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.Connections
{
    /// <inheritdoc />
    internal class SniConnectionEndPoint : IConnectionEndPoint
    {
        private readonly string _serverName;
        private readonly IPEndPoint _hostIpEndPoint;

        public SniConnectionEndPoint(IPEndPoint socketIpEndPoint, string serverName) : 
            this(socketIpEndPoint, null, serverName)
        {
        }

        public SniConnectionEndPoint(IPEndPoint socketIpEndPoint, IPEndPoint hostIpEndPoint, string serverName)
        {
            SocketIpEndPoint = socketIpEndPoint;
            _hostIpEndPoint = hostIpEndPoint;
            _serverName = serverName;

            var stringBuilder = new StringBuilder(hostIpEndPoint?.ToString() ?? socketIpEndPoint.ToString());

            if (hostIpEndPoint == null && serverName != null)
            {
                stringBuilder.Append($" ({serverName})");
            }

            EndpointFriendlyName = stringBuilder.ToString();
        }

        /// <inheritdoc />
        public IPEndPoint SocketIpEndPoint { get; }

        /// <inheritdoc />
        public string EndpointFriendlyName { get; }
        
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
        public IPEndPoint GetOrParseHostIpEndPoint(Row row, IAddressTranslator translator, int port)
        {
            if (_hostIpEndPoint != null)
            {
                return _hostIpEndPoint;
            }

            var ipEndPoint = ControlConnection.GetAddressForLocalOrPeerHost(row, translator, port);
            if (ipEndPoint == null)
            {
                throw new DriverInternalError("Could not parse the node's ip address from system tables.");
            }

            return ipEndPoint;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SniConnectionEndPoint point))
            {
                return false;
            }

            if (_hostIpEndPoint != null)
            {
                if (point._hostIpEndPoint == null || !_hostIpEndPoint.Equals(point._hostIpEndPoint))
                {
                    return false;
                }
            }
            else if (point._hostIpEndPoint != null)
            {
                return false;
            }

            return _serverName == point._serverName && SocketIpEndPoint.Equals(point.SocketIpEndPoint);
        }

        public override int GetHashCode()
        {
            var objList = new List<object>();

            if (_hostIpEndPoint != null)
            {
                objList.Add(_hostIpEndPoint);
            }

            if (_serverName != null)
            {
                objList.Add(_serverName);
            }

            if (SocketIpEndPoint != null)
            {
                objList.Add(SocketIpEndPoint);
            }

            return Utils.CombineHashCode(objList);
        }
    }
}