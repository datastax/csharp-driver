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

using System.Net.Sockets;
using System.Threading.Tasks;

namespace Cassandra.Connections.Control
{
    /// <summary>
    /// Gets the next connection and setup the event listener for the host and connection.
    /// </summary>
    /// <exception cref="SocketException" />
    /// <exception cref="DriverInternalError" />
    internal interface IServerEventsSubscriber
    {
        Task SubscribeToServerEvents(IConnection connection, CassandraEventHandler handler);
    }
}