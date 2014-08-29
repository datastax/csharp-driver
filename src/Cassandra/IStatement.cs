//
//      Copyright (C) 2012-2014 DataStax Inc.
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

﻿using System;
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
        byte[] PagingState { get; }
        object[] QueryValues { get; }
        /// <summary>
        /// Gets the timestamp associated with this statement execution.
        /// </summary>
        DateTimeOffset? Timestamp { get; }
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
        ConsistencyLevel SerialConsistencyLevel { get; }
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
        IStatement SetPagingState(byte[] pagingState);
        /// <summary>
        ///  Sets the retry policy to use for this query. <p> The default retry policy, if
        ///  this method is not called, is the one returned by
        ///  <link>Policies#RetryPolicy</link> in the
        ///  cluster configuration. This method is thus only useful in case you want to
        ///  punctually override the default policy for this request.</p>
        /// </summary>
        /// <param name="policy"> the retry policy to use for this query. </param>
        /// <returns>this <c>IStatement</c> object.</returns>
        IStatement SetRetryPolicy(IRetryPolicy policy);
        /// <summary>
        /// Sets the serial consistency level for the query.
        ///    The serial consistency level is only used by conditional updates (so INSERT, UPDATE
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
        bool SkipMetadata { get; }
    }
}
