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
using System.Net.Security;
using System.Threading.Tasks;
using Cassandra.Connections.Control;

namespace Cassandra.Connections
{
    /// <summary>
    /// Represents a remote EndPoint that can be used to open a connection to a specific Host or ContactPoint.
    /// In some scenarios, the socket IpEndPoint might be different from the IpEndPoint of the Host. For example, scenarios
    /// where a proxy is between the host and the driver. This abstraction makes the implementation of <see cref="IConnection"/>
    /// decoupled from the Host's IpEndPoint which can be obtained via DNS resolution or via system.peers queries.
    /// </summary>
    internal interface IConnectionEndPoint : IEquatable<IConnectionEndPoint>
    {
        /// <summary>
        /// ContactPoint from which this endpoint was resolved. It is null if it was parsed from system tables.
        /// </summary>
        IContactPoint ContactPoint { get; }

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
        /// Gets the Host IpEndPoint associated with this endpoint. If there is none, return null.
        /// </summary>
        IPEndPoint GetHostIpEndPoint();
    }
}