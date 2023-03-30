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
using System.Net;

namespace Cassandra.Connections
{
    /// <inheritdoc />
    internal class ServerNameResolver : IServerNameResolver
    {
        private readonly ProtocolOptions _protocolOptions;

        public ServerNameResolver(ProtocolOptions protocolOptions)
        {
            _protocolOptions = protocolOptions ?? throw new ArgumentNullException(nameof(protocolOptions));
        }

        /// <inheritdoc />
        public string GetServerName(IPEndPoint socketIpEndPoint)
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
    }
}