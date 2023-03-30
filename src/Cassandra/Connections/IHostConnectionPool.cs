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
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Cassandra.Connections
{
    internal interface IHostConnectionPool : IDisposable
    {
        /// <summary>
        /// Gets the total amount of open connections. 
        /// </summary>
        int OpenConnections { get; }

        /// <summary>
        /// Gets the total of in-flight requests on all connections. 
        /// </summary>
        int InFlight { get; }
        
        /// <summary>
        /// Determines whether the connection pool has opened connections using snapshot semantics.
        /// </summary>
        bool HasConnections { get; }

        event Action<Host, HostConnectionPool> AllConnectionClosed;
        
        /// <summary>
        /// Gets a snapshot of the current state of the pool.
        /// </summary>
        IConnection[] ConnectionsSnapshot { get; }
        
        /// <summary>
        /// Gets an open connection from the host pool (creating if necessary).
        /// It returns null if the load balancing policy didn't allow connections to this host.
        /// </summary>
        /// <exception cref="DriverInternalError" />
        /// <exception cref="BusyPoolException" />
        /// <exception cref="UnsupportedProtocolVersionException" />
        /// <exception cref="SocketException" />
        /// <exception cref="AuthenticationException" />
        Task<IConnection> BorrowConnectionAsync();

        /// <summary>
        /// Gets an open connection from the host pool. It does NOT create one if necessary (for that use <see cref="BorrowConnectionAsync"/>.
        /// It returns null if there isn't a connection available.
        /// </summary>
        /// <exception cref="BusyPoolException" />
        /// <exception cref="SocketException">Not connected.</exception>
        IConnection BorrowExistingConnection();

        void SetDistance(HostDistance distance);

        void CheckHealth(IConnection connection);

        /// <summary>
        /// Closes the connection and removes it from the pool
        /// </summary>
        void Remove(IConnection c);

        /// <summary>
        /// Adds a new reconnection timeout using a new schedule.
        /// Resets the status of the pool to allow further reconnections.
        /// </summary>
        void ScheduleReconnection(bool immediate = false);

        /// <summary>
        /// Creates the required connections to the hosts and awaits for all connections to be open.
        /// The task is completed when at least 1 of the connections is opened successfully.
        /// Until the task is completed, no other thread is expected to be using this instance.
        /// </summary>
        Task Warmup();

        void OnHostRemoved();

        void MarkAsDownAndScheduleReconnection();

        Task<IConnection> GetConnectionFromHostAsync(
            IDictionary<IPEndPoint, Exception> triedHosts, Func<string> getKeyspaceFunc);
        
        Task<IConnection> GetExistingConnectionFromHostAsync(
            IDictionary<IPEndPoint, Exception> triedHosts, Func<string> getKeyspaceFunc);
    }
}