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

using System.Net;
using System.Net.Security;

namespace Cassandra.Connections
{
    /// <summary>
    /// Reverse resolver. Used to obtain the server name from a given IP endpoint. The returned server
    /// name will then be used in <see cref="SslStream.AuthenticateAsClientAsync(string)"/> for SSL and SNI for example.
    /// </summary>
    internal interface IServerNameResolver
    {
        /// <summary>
        /// Performs reverse resolution to obtain the server name from the given ip endpoint.
        /// </summary>
        string GetServerName(IPEndPoint socketIpEndPoint);
    }
}