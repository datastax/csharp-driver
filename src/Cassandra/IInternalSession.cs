﻿//
//      Copyright DataStax, Inc.
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

namespace Cassandra
{
    using System.Collections.Generic;
    using System.Net;
    using System.Threading.Tasks;

    /// <inheritdoc />
    /// <remarks>This is an internal interface designed to declare the internal methods that are called
    /// across multiple locations of the driver's source code.</remarks>
    internal interface IInternalSession : ISession
    {
        /// <summary>
        /// Initialize the session
        /// </summary>
        Task Init();

        /// <summary>
        /// Gets or creates the connection pool for a given host
        /// </summary>
        HostConnectionPool GetOrCreateConnectionPool(Host host, HostDistance distance);

        /// <summary>
        /// Gets a snapshot of the connection pools
        /// </summary>
        KeyValuePair<IPEndPoint, HostConnectionPool>[] GetPools();

        /// <summary>
        /// Gets the existing connection pool for this host and session or null when it does not exists
        /// </summary>
        HostConnectionPool GetExistingPool(IPEndPoint address);

        void CheckHealth(IConnection connection);

        bool HasConnections(Host host);

        void MarkAsDownAndScheduleReconnection(Host host, HostConnectionPool pool);

        void OnAllConnectionClosed(Host host, HostConnectionPool pool);

        /// <summary>
        /// Gets or sets the keyspace
        /// </summary>
        new string Keyspace { get; set; }
    }
}