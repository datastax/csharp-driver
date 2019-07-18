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

using System.Net;
using System.Net.Security;
using System.Threading.Tasks;

namespace Dse.Connections
{
    /// <summary>
    /// Represents a remote EndPoint that can be used to open a connection to a specific Host or ContactPoint.
    /// In some scenarios, the socket IpEndPoint might be different from the IpEndPoint of the Host. For example, scenarios
    /// where a proxy is between the host and the driver. This abstraction makes the implementation of <see cref="IConnection"/>
    /// decoupled from the Host's IpEndPoint which can be obtained via DNS resolution or via system.peers queries.
    /// </summary>
    internal interface IConnectionEndPoint
    {
        /// <summary>
        /// IpEndPoint to which the driver will connect to (via <see cref="ITcpSocket"/>). This can never be null.
        /// </summary>
        IPEndPoint SocketIpEndPoint { get; }
        
        /// <summary>
        /// Useful for logging purposes.
        /// </summary>
        string EndpointFriendlyName { get; }

        /// <summary>
        /// Server name of the host, in the context of TLS and SNI. The value returned by this method is meant to be used
        /// in <see cref="ITcpSocket"/> when opening a TLS connection. Also, see the parameter of <see cref="SslStream.AuthenticateAsClientAsync(string)"/>,
        /// which is used to verify the host name of the certificate but is also used as the Server Name in SNI (SNI is always enabled).
        /// </summary>
        Task<string> GetServerNameAsync();

        /// <summary>
        /// There are cases where the Host's IpEndPoint is required but it's not always available so this method
        /// is meant to be used in cases where the caller absolutely needs an <see cref="IPEndPoint"/> to identify the host even
        /// though it might not necessarily be the host's private IP address.
        /// </summary>
        IPEndPoint GetHostIpEndPointWithFallback();

        /// <summary>
        /// Gets the Host IpEndPoint associated with this endpoint. If there is none, then parse it from the provided row.
        /// This row should be the result of a SELECT statement on the system.local table.
        /// </summary>
        /// <param name="row">Result from the query on system.local table.</param>
        /// <param name="translator">Address translator to use when parsing the host's IP address from the <paramref name="row"/>.</param>
        /// <param name="port">Port to use when building the <see cref="IPEndPoint"/> in case the IP address is parsed from the <paramref name="row"/>.</param>
        /// <returns></returns>
        IPEndPoint GetOrParseHostIpEndPoint(Row row, IAddressTranslator translator, int port);
    }
}