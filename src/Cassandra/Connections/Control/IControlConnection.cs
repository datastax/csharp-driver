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
using System.Threading.Tasks;

namespace Cassandra.Connections.Control
{
    internal interface IControlConnection : IMetadataQueryProvider, IDisposable
    {
        /// <summary>
        /// Host to which the control connection is currently connected.
        /// </summary>
        Host Host { get; }
        
        /// <summary>
        /// Tries to create a connection to any of the contact points and retrieve cluster metadata for the first time.
        /// Not thread-safe.
        /// </summary>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="TimeoutException" />
        /// <exception cref="DriverInternalError" />
        Task InitAsync();
        
        /// <summary>
        /// Updates keyspace metadata and token map if necessary.
        /// </summary>
        Task HandleSchemaChangeEvent(SchemaChangeEventArgs ssc, bool processNow);

        /// <summary>
        /// Schedule a keyspace refresh. The returned task will be complete when the refresh is done.
        /// Currently only used in tests.
        /// </summary>
        Task HandleKeyspaceRefreshLaterAsync(string keyspace);

        /// <summary>
        /// Schedule a keyspace refresh. If <paramref name="processNow"/> is <code>true</code>,
        /// the returned task will be complete when the refresh is done. If it's <code>false</code>
        /// then the returned task will be complete when the refresh has been added to the queue (event debouncer).
        /// </summary>
        Task ScheduleKeyspaceRefreshAsync(string keyspace, bool processNow);
        
        /// <summary>
        /// Schedule a refresh of all keyspaces. If <paramref name="processNow"/> is <code>true</code>,
        /// the returned task will be complete when the refresh is done. If it's <code>false</code>
        /// then the returned task will be complete when the refresh has been added to the queue (event debouncer).
        /// </summary>
        Task ScheduleAllKeyspacesRefreshAsync(bool processNow);
    }
}