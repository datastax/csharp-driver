//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    /// Base class for statements that contains the options.
    /// </summary>
    public abstract class Statement : IStatement
    {
        private ConsistencyLevel? _consistency;
        private int _pageSize;
        private byte[] _pagingState;
        private IRetryPolicy _retryPolicy;
        private ConsistencyLevel _serialConsistency;
        private bool _skipMetadata;
        private bool _traceQuery;
        private object[] _values;

        public virtual object[] QueryValues
        {
            get { return _values; }
        }

        public bool SkipMetadata
        {
            get { return _skipMetadata; }
        }

        public ConsistencyLevel? ConsistencyLevel
        {
            get { return _consistency; }
        }

        /// <summary>
        /// Gets the serial consistency level for this query.
        /// </summary>        
        public ConsistencyLevel SerialConsistencyLevel
        {
            get { return _serialConsistency; }
        }

        public int PageSize
        {
            get { return _pageSize; }
        }

        public bool IsTracing
        {
            get { return _traceQuery; }
        }

        public IRetryPolicy RetryPolicy
        {
            get { return _retryPolicy; }
        }

        public byte[] PagingState
        {
            get { return _pagingState; }
        }

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
        /// <returns>the routing key for this query or <c>null</c>.</returns>
        public abstract RoutingKey RoutingKey { get; }


        // We don't want to expose the constructor, because the code rely on this being only subclassed by Statement and BoundStatement
        protected Statement()
        {
            //this._consistency = QueryOptions.DefaultConsistencyLevel;
        }

        protected Statement(QueryProtocolOptions queryProtocolOptions)
        {
            _pagingState = queryProtocolOptions.PagingState;
            _values = queryProtocolOptions.Values;
            _consistency = queryProtocolOptions.Consistency;
            _pageSize = queryProtocolOptions.PageSize;
            _serialConsistency = queryProtocolOptions.SerialConsistency;
        }

        internal Statement SetSkipMetadata(bool val)
        {
            _skipMetadata = val;
            return this;
        }

        /// <summary>
        ///  Bound values to the variables of this statement. This method provides a
        ///  convenience to bound all the variables of the <c>BoundStatement</c> in
        ///  one call.
        /// </summary>
        /// <param name="values"> the values to bind to the variables of the newly
        ///  created BoundStatement. The first element of <c>values</c> will 
        ///  be bound to the first bind variable,
        ///  etc.. It is legal to provide less values than the statement has bound
        ///  variables. In that case, the remaining variable need to be bound before
        ///  execution. If more values than variables are provided however, an
        ///  IllegalArgumentException wil be raised. </param>
        /// 
        /// <returns>this bound statement. </returns>
        internal IStatement SetValues(object[] values)
        {
            _values = values;
            return this;
        }

        public IStatement SetPagingState(byte[] pagingState)
        {
            _pagingState = pagingState;
            return this;
        }

        /// <summary>
        ///  Sets the consistency level for the query. <p> The default consistency level,
        ///  if this method is not called, is ConsistencyLevel.ONE.</p>
        /// </summary>
        /// <param name="consistency"> the consistency level to set. </param>
        /// 
        /// <returns>this <c>Query</c> object.</returns>
        public IStatement SetConsistencyLevel(ConsistencyLevel? consistency)
        {
            _consistency = consistency;
            return this;
        }

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
        /// <returns>this <c>Query</c> object.</returns>
        public IStatement SetSerialConsistencyLevel(ConsistencyLevel serialConsistency)
        {
            if (serialConsistency != Cassandra.ConsistencyLevel.Serial && serialConsistency != Cassandra.ConsistencyLevel.LocalSerial)
                throw new ArgumentException("The serial consistency can only be set to ConsistencyLevel.LocalSerial or ConsistencyLevel.Serial.");

            _serialConsistency = serialConsistency;
            return this;
        }

        /// <summary>
        ///  Enable tracing for this query. By default (i.e. unless you call this method),
        ///  tracing is not enabled.
        /// </summary>
        /// 
        /// <returns>this <c>Query</c> object.</returns>
        public IStatement EnableTracing(bool enable = true)
        {
            _traceQuery = enable;
            return this;
        }

        public IStatement DisableTracing()
        {
            _traceQuery = false;
            return this;
        }


        /// <summary>
        ///  Sets the retry policy to use for this query. <p> The default retry policy, if
        ///  this method is not called, is the one returned by
        ///  <link>Policies#RetryPolicy</link> in the
        ///  cluster configuration. This method is thus only useful in case you want to
        ///  punctually override the default policy for this request.</p>
        /// </summary>
        /// <param name="policy"> the retry policy to use for this query. </param>
        /// 
        /// <returns>this <c>Query</c> object.</returns>
        public IStatement SetRetryPolicy(IRetryPolicy policy)
        {
            _retryPolicy = policy;
            return this;
        }

        internal virtual IQueryRequest CreateBatchRequest()
        {
            throw new InvalidOperationException("Cannot insert this query into the batch");
        }

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
        /// Also note that only {@code SELECT} queries ever make use of that
        /// setting.
        /// </p>
        ///
        /// @param fetchSize the fetch size to use. If {@code fetchSize &lt;= 0},
        /// the default fetch size will be used. To disable paging of the
        /// result set, use {@code fetchSize == Integer.MAX_VALUE}.
        /// @return this {@code Statement} object.
        /// </summary>
        /// <param name="pageSize">the page size to use. If set to 0 or less, the default value will be used.
        /// To disable paging of the result set, use int.MaxValue</param>
        /// <returns>this <c>Query</c> object.</returns>
        public IStatement SetPageSize(int pageSize)
        {
            _pageSize = pageSize;
            return this;
        }
    }
}