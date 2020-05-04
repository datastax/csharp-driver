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

using System.Threading.Tasks;
using Cassandra.Serialization;

namespace Cassandra.Connections.Control
{
    /// <summary>
    /// Class that issues system table queries and updates the hosts collection on <see cref="Metadata"/>.
    /// </summary>
    internal interface ITopologyRefresher
    {
        /// <summary>
        /// Refreshes the Hosts collection using the <paramref name="currentEndPoint"/> to issue system table queries (local and peers).
        /// </summary>
        /// <returns>Returns the Host parsed from the <paramref name="currentEndPoint"/>'s system.local table.</returns>
        Task<Host> RefreshNodeListAsync(IConnectionEndPoint currentEndPoint, IConnection connection, ISerializer serializer);
    }
}