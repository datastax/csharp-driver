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
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    /// <summary>
    /// Handles request executions, each execution handles retry and failover.
    /// </summary>
    internal interface IRequestHandler
    {
        IRequestOptions RequestOptions { get; }

        IExtendedRetryPolicy RetryPolicy { get; }

        ISerializer Serializer { get; }

        IStatement Statement { get; }

        /// <summary>
        /// Marks this instance as completed (if not already) and sets the exception or result
        /// </summary>
        bool SetCompleted(Exception ex, RowSet result = null);

        /// <summary>
        /// Marks this instance as completed (if not already) and in a new Task using the default scheduler, it invokes the action and sets the result
        /// </summary>
        bool SetCompleted(RowSet result, Action action);

        void SetNoMoreHosts(NoHostAvailableException ex, IRequestExecution execution);

        bool HasCompleted();
        
        /// <summary>
        /// Gets the next valid host for the purpose of obtaining a connection.
        /// </summary>
        /// <param name="triedHosts">Hosts for which there were attempts to connect and send the request.</param>
        /// <exception cref="NoHostAvailableException">If every host from the query plan is unavailable.</exception>
        /// <returns>This method always returns non-null <code>ValidHost</code></returns>
        ValidHost GetNextValidHost(Dictionary<IPEndPoint, Exception> triedHosts);

        /// <summary>
        /// Chooses the next valid host according to the load balancing policy and query plan and gets a connection to that host.
        /// </summary>
        /// <param name="triedHosts">Hosts for which there were attempts to connect and send the request.</param>
        /// <exception cref="InvalidQueryException">When the keyspace is not valid</exception>
        /// <exception cref="NoHostAvailableException">If every host from the query plan is unavailable.</exception>
        Task<IConnection> GetNextConnectionAsync(Dictionary<IPEndPoint, Exception> triedHosts);

        /// <summary>
        /// Gets a connection to a provided host or <code>null</code> if its not possible, filling the <paramref name="triedHosts"/> map with the failures.
        /// <para></para>
        /// This method assumes the host has been previously validated.
        /// </summary>
        /// <param name="validHost">Host to which a connection will be obtained.</param>
        /// <param name="triedHosts">Hosts for which there were attempts to connect and send the request.</param>
        /// <exception cref="InvalidQueryException">When the keyspace is not valid</exception>
        /// <exception cref="NoHostAvailableException">If every host from the query plan is unavailable.</exception>
        Task<IConnection> GetConnectionToValidHostAsync(ValidHost validHost, IDictionary<IPEndPoint, Exception> triedHosts);

        /// <summary>
        /// Obtain a connection to the provided <paramref name="host"/>.
        /// This method validates the <paramref name="host"/> before getting a connection.
        /// </summary>
        /// <param name="host">Host to which a connection will be obtained.</param>
        /// <param name="triedHosts">Hosts for which there were attempts to connect and send the request.</param>
        /// <exception cref="InvalidQueryException">When the keyspace is not valid</exception>
        /// <exception cref="NoHostAvailableException">If every host from the query plan is unavailable.</exception>
        Task<IConnection> ValidateHostAndGetConnectionAsync(Host host, Dictionary<IPEndPoint, Exception> triedHosts);

        Task<RowSet> SendAsync();

        /// <summary>
        /// Builds the Request to send to a cassandra node based on the statement type
        /// </summary>
        IRequest BuildRequest();
    }
}