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

namespace Cassandra
{
    /// <summary>
    ///  An executable query.
    ///  This represents either a <see cref="SimpleStatement"/>, a <see cref="BoundStatement"/> or a
    ///  <see cref="BoundStatement"/> along with the query options (consistency level,
    ///  whether to trace the query, ...).
    /// </summary>
    public interface IStatement
    {
        /// <summary>
        /// Determines if the <see cref="RowSet"/> returned when executing this <c>IStatement</c> will automatically fetch the following result pages. Defaults to true.
        /// </summary>
        bool AutoPage { get; }

        /// <summary>
        /// Gets the consistency level for this query.
        /// </summary>
        ConsistencyLevel? ConsistencyLevel { get; }

        /// <summary>
        ///  Disable tracing for the statement.
        /// </summary>
        IStatement DisableTracing();

        /// <summary>
        ///  Enables tracing for the statement
        /// </summary>
        IStatement EnableTracing(bool enable = true);

        /// <summary>
        ///  Gets whether tracing is enabled for this query or not.
        /// </summary>
        bool IsTracing { get; }

        /// <summary>
        /// Gets query's page size.
        /// </summary>
        int PageSize { get; }

        /// <summary>
        /// This object represents the next page to be fetched if the query is multi page. It can be saved and reused
        /// later on a different execution.
        /// </summary>
        byte[] PagingState { get; }

        object[] QueryValues { get; }

        /// <summary>
        /// Gets the timestamp associated with this statement execution.
        /// </summary>
        DateTimeOffset? Timestamp { get; }

        /// <summary>
        /// Gets the per-host read timeout for this statement.
        /// <para>
        /// When the value is <c>0</c> or lower, the default value from the socket options will be used.
        /// </para>
        /// </summary>
        int ReadTimeoutMillis { get; }

        /// <summary>
        ///  Gets the retry policy sets for this query, if any.
        /// </summary>
        IRetryPolicy RetryPolicy { get; }

        /// <summary>
        ///  The routing key (in binary raw form) to use for token aware routing of this
        ///  query. <p> The routing key is optional in the sense that implementers are
        ///  free to return <c>null</c>. The routing key is an hint used for token
        ///  aware routing (see
        ///  <link>TokenAwarePolicy</link>), and if
        ///  provided should correspond to the binary value for the query partition key.
        ///  However, not providing a routing key never causes a query to fail and if the
        ///  load balancing policy used is not token aware, then the routing key can be
        ///  safely ignored.</p>
        /// </summary>
        RoutingKey RoutingKey { get; }

        /// <summary>
        /// Gets the serial consistency level for the query.
        /// <para>
        /// The serial consistency level is only used by conditional updates (INSERT, UPDATE
        /// and DELETE with an IF condition).
        /// </para>
        /// </summary>

        ConsistencyLevel SerialConsistencyLevel { get; }

        bool SkipMetadata { get; }

        /// <summary>
        /// Gets custom payload for that will be included when executing this Statement.
        /// </summary>
        IDictionary<string, byte[]> OutgoingPayload { get; }

        /// <summary>
        /// Determines if this statement is idempotent, i.e. whether it can be applied multiple times without 
        /// changing the result beyond the initial application.
        /// <para>
        /// Idempotence of the statement plays a role in <see cref="ISpeculativeExecutionPolicy"/>.
        /// If a statement is <em>not idempotent</em>, the driver will not schedule speculative executions for it.
        /// </para>
        /// When the property is null, the driver will use the default value from the <see cref="QueryOptions.GetDefaultIdempotence()"/>.
        /// </summary>
        bool? IsIdempotent { get; }

        /// <summary>
        /// Returns the keyspace this query operates on.
        /// <para>
        /// Note that not all <see cref="Statement"/> implementations specify on which keyspace they operate on
        /// so this method can return null. If null, it will operate on the default keyspace set during initialization (if it was set).
        /// </para>
        /// <para>
        /// The keyspace returned is used as a hint for token-aware routing.
        /// </para>
        /// </summary>
        /// <remarks>
        /// Consider using a <see cref="ISession"/> connected to single keyspace using 
        /// <see cref="ICluster.Connect(string)"/>.
        /// </remarks>
        string Keyspace { get; }

        /// <summary>
        /// Allows this statement to be executed as a different user/role than the one 
        /// currently authenticated (a.k.a. proxy execution).
        /// </summary>
        /// <param name="userOrRole">The user or role name to act as when executing this statement.</param>
        /// <returns>This statement</returns>
        /// <remarks>This feature is only available in DSE 5.1+.</remarks>
        IStatement ExecutingAs(string userOrRole);

        /// <summary>
        /// Sets the paging behavior.
        /// When set to true (default), the <see cref="RowSet"/> returned when executing this <c>IStatement</c> will automatically fetch the following result pages.
        /// When false, the <see cref="RowSet"/> returned will only contain the rows contained in the result page and will not fetch additional pages.
        /// </summary>
        /// <returns>this <c>IStatement</c> object.</returns>
        IStatement SetAutoPage(bool autoPage);

        /// <summary>
        ///  Sets the consistency level for the query. <p> The default consistency level,
        ///  if this method is not called, is ConsistencyLevel.ONE.</p>
        /// </summary>
        /// <param name="consistency"> the consistency level to set. </param>
        /// <returns>this <c>IStatement</c> object.</returns>
        IStatement SetConsistencyLevel(ConsistencyLevel? consistency);

        /// <summary>
        /// Sets the page size for this query.
        /// The page size controls how much resulting rows will be retrieved
        /// simultaneously (the goal being to avoid loading too much results
        /// in memory for queries yielding large results). Please note that
        /// while value as low as 1 can be used, it is highly discouraged to
        /// use such a low value in practice as it will yield very poor
        /// performance. If in doubt, leaving the default is probably a good
        /// idea.
        /// <p>
        /// Also note that only <c>SELECT</c> queries ever make use of that
        /// setting.
        /// </p>
        /// <param name="pageSize">the page size to use. If set to 0 or less, the default value will be used.
        /// To disable paging of the result set, use int.MaxValue</param>
        /// <returns>this <c>Query</c> object.</returns>
        /// </summary>
        IStatement SetPageSize(int pageSize);

        /// <summary>
        /// Sets the paging state, a token representing the current page state of query used to continue paging by retrieving the following result page.
        /// Setting the paging state will disable automatic paging.
        /// </summary>
        /// <param name="pagingState">The page state token</param>
        /// <returns>this <c>IStatement</c> object.</returns>
        IStatement SetPagingState(byte[] pagingState);

        /// <summary>
        /// Overrides the default per-host read timeout <see cref="SocketOptions.ReadTimeoutMillis"/> for this statement.
        /// </summary>
        /// <param name="timeout">
        /// Timeout in milliseconds. If the value is not greater than zero, the default value 
        /// from the socket options will be used.
        /// </param>
        IStatement SetReadTimeoutMillis(int timeout);

        /// <summary>
        /// Sets the retry policy to use for this query.
        /// <para>
        /// Calling this method is only required when you want to override the default 
        /// <see cref="Policies.RetryPolicy"/> set in the cluster configuration for this request or the one set
        /// in the execution profile (see <see cref="IExecutionProfile.RetryPolicy"/>) for this request.
        /// </para>
        /// <para>
        /// Use a <see cref="IExtendedRetryPolicy"/> implementation to cover all error scenarios.
        /// </para>
        /// </summary>
        /// <param name="policy">The retry policy to use for this query.</param>
        /// <returns>this <see cref="IStatement"/> instance.</returns>
        IStatement SetRetryPolicy(IRetryPolicy policy);

        /// <summary>
        /// Sets the serial consistency level for the query.
        /// The serial consistency level is only used by conditional updates (so INSERT, UPDATE
        /// and DELETE with an IF condition). For those, the serial consistency level defines
        /// the consistency level of the serial phase (or "paxos" phase) while the
        /// normal consistency level defines the consistency for the "learn" phase, i.e. what
        /// type of reads will be guaranteed to see the update right away. For instance, if
        /// a conditional write has a regular consistency of QUORUM (and is successful), then a
        /// QUORUM read is guaranteed to see that write. But if the regular consistency of that
        /// write is ANY, then only a read with a consistency of SERIAL is guaranteed to see it
        /// (even a read with consistency ALL is not guaranteed to be enough).
        /// </summary>
        /// <param name="serialConsistency">Can be set only to ConsistencyLevel.Serial or 
        /// ConsistencyLevel.LocalSerial. Setting it to ConsistencyLevel.Serial guarantees full 
        /// linearizability while ConsistencyLevel.LocalSerial guarantees it only in the local datacenter. </param>
        /// <returns>this <c>IStatement</c> object.</returns>
        IStatement SetSerialConsistencyLevel(ConsistencyLevel serialConsistency);

        /// <summary>
        /// Sets the timestamp associated with this statement execution.
        /// If provided, this will replace the server side assigned 
        /// timestamp as default timestamp. Note that a timestamp in the query itself will still override this timestamp.
        /// </summary>
        IStatement SetTimestamp(DateTimeOffset value);

        /// <summary>
        /// Sets a custom outgoing payload for this statement.
        /// Each time this statement is executed, this payload will be included in the request.
        /// Once it is set using this method, the payload should not be modified.
        /// </summary>
        IStatement SetOutgoingPayload(IDictionary<string, byte[]> payload);

        /// <summary>
        /// Sets whether this statement is idempotent.
        /// <para>
        /// Idempotence of the statement plays a role in <see cref="ISpeculativeExecutionPolicy"/>.
        /// If a statement is <em>not idempotent</em>, the driver will not schedule speculative executions for it.
        /// </para>
        /// </summary>
        IStatement SetIdempotence(bool value);
    }
}
