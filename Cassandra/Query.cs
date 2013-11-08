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
﻿using System;

namespace Cassandra
{
    /// <summary>
    ///  An executable query. <p> This represents either a <link>Statement</link> or a
    ///  <link>BoundStatement</link> along with the query options (consistency level,
    ///  whether to trace the query, ...).</p>
    /// </summary>
    public abstract class Query
    {
        private ConsistencyLevel? _consistency;
        private ConsistencyLevel _serialConsistency;
        private bool _traceQuery;
        private int _pageSize;
        private byte[] _pagingState;
        private IRetryPolicy _retryPolicy;
        private object[] _values;
        private bool _skipMetadata;

        public virtual object[] QueryValues
        {
            get { return _values; }
        }

        public bool SkipMetadata { get { return this._skipMetadata; } }
        internal Query SetSkipMetadata(bool val)
        {
            this._skipMetadata = val;
            return this;
        }

        /// <summary>
        ///  Bound values to the variables of this statement. This method provides a
        ///  convenience to bound all the variables of the <code>BoundStatement</code> in
        ///  one call.
        /// </summary>
        /// <param name="values"> the values to bind to the variables of the newly
        ///  created BoundStatement. The first element of <code>values</code> will 
        ///  be bound to the first bind variable,
        ///  etc.. It is legal to provide less values than the statement has bound
        ///  variables. In that case, the remaining variable need to be bound before
        ///  execution. If more values than variables are provided however, an
        ///  IllegalArgumentException wil be raised. </param>
        /// 
        /// <returns>this bound statement. </returns>
        internal Query SetValues(object[] values)
        {
            this._values = values;
            return this;
        }

        /// <summary>
        /// Gets the consistency level for this query.
        /// </summary>
        public ConsistencyLevel? ConsistencyLevel { get { return _consistency; } }
     
        /// <summary>
        /// Gets the serial consistency level for this query.
        /// </summary>        
        public ConsistencyLevel SerialConsistencyLevel { get { return _serialConsistency; } }
        
        /// <summary>
        /// Gets query's page size.
        /// </summary>
        public int PageSize { get { return _pageSize; } }

        /// <summary>
        ///  Gets whether tracing is enabled for this query or not.
        /// </summary>
        public bool IsTracing { get { return _traceQuery; } }

        /// <summary>
        ///  Gets the retry policy sets for this query, if any.
        /// </summary>
        public IRetryPolicy RetryPolicy { get { return _retryPolicy; } }


        // We don't want to expose the constructor, because the code rely on this being only subclassed by Statement and BoundStatement
        protected Query()
        {
            //this._consistency = QueryOptions.DefaultConsistencyLevel;
        }

        protected Query(QueryProtocolOptions queryProtocolOptions)
        {
            this._pagingState = queryProtocolOptions.PagingState;
            this._values = queryProtocolOptions.Values;
            this._consistency = queryProtocolOptions.Consistency;
            this._pageSize = queryProtocolOptions.PageSize;
            this._serialConsistency = queryProtocolOptions.SerialConsistency;
        }

        public Query SetPagingState(byte[] pagingState)
        {
            this._pagingState = pagingState;
            return this;
        }

        public byte[] PagingState { get { return this._pagingState; } }

        /// <summary>
        ///  Sets the consistency level for the query. <p> The default consistency level,
        ///  if this method is not called, is ConsistencyLevel.ONE.</p>
        /// </summary>
        /// <param name="consistency"> the consistency level to set. </param>
        /// 
        /// <returns>this <code>Query</code> object.</returns>
        public Query SetConsistencyLevel(ConsistencyLevel? consistency)
        {
            this._consistency = consistency;
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
        /// <returns>this <code>Query</code> object.</returns>
        public Query SetSerialConsistencyLevel(ConsistencyLevel serialConsistency)
        {
            if (serialConsistency != Cassandra.ConsistencyLevel.Serial && serialConsistency != Cassandra.ConsistencyLevel.LocalSerial)
                throw new ArgumentException("The serial consistency can only be set to ConsistencyLevel.LocalSerial or ConsistencyLevel.Serial.");

            this._serialConsistency = serialConsistency;
            return this;
        }

        /// <summary>
        ///  Enable tracing for this query. By default (i.e. unless you call this method),
        ///  tracing is not enabled.
        /// </summary>
        /// 
        /// <returns>this <code>Query</code> object.</returns>
        public Query EnableTracing(bool enable=true)
        {
            this._traceQuery = enable;
            return this;
        }

        /// <summary>
        ///  Disable tracing for this query.
        /// </summary>
        /// 
        /// <returns>this <code>Query</code> object.</returns>
        public Query DisableTracing()
        {
            this._traceQuery = false;
            return this;
        }

        /// <summary>
        ///  The routing key (in binary raw form) to use for token aware routing of this
        ///  query. <p> The routing key is optional in the sense that implementers are
        ///  free to return <code>null</code>. The routing key is an hint used for token
        ///  aware routing (see
        ///  <link>TokenAwarePolicy</link>), and if
        ///  provided should correspond to the binary value for the query partition key.
        ///  However, not providing a routing key never causes a query to fail and if the
        ///  load balancing policy used is not token aware, then the routing key can be
        ///  safely ignored.</p>
        /// </summary>
        /// 
        /// <returns>the routing key for this query or <code>null</code>.</returns>
        public abstract RoutingKey RoutingKey { get; }


        /// <summary>
        ///  Sets the retry policy to use for this query. <p> The default retry policy, if
        ///  this method is not called, is the one returned by
        ///  <link>Policies#RetryPolicy</link> in the
        ///  cluster configuration. This method is thus only useful in case you want to
        ///  punctually override the default policy for this request.</p>
        /// </summary>
        /// <param name="policy"> the retry policy to use for this query. </param>
        /// 
        /// <returns>this <code>Query</code> object.</returns>
        public Query SetRetryPolicy(IRetryPolicy policy)
        {
            this._retryPolicy = policy;
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
        ///
        /// @param fetchSize the fetch size to use. If {@code fetchSize &lte; 0},
        /// the default fetch size will be used. To disable paging of the
        /// result set, use {@code fetchSize == Integer.MAX_VALUE}.
        /// @return this {@code Statement} object.
        ///
        /// </summary>
        /// <param name="pageSize">the page size to use. If set to 0 or less, the default value will be used.
        /// To disable paging of the result set, use int.MaxValue</param>
        /// <returns>this <code>Query</code> object.</returns>
        public Query SetPageSize(int pageSize)
        {            
            this._pageSize = pageSize;
            return this;
        }

        protected internal abstract IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state);

        protected internal abstract RowSet EndSessionExecute(Session session, IAsyncResult ar);

    }
}
