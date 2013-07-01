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
        private volatile ConsistencyLevel _consistency;
        private volatile bool _traceQuery;

        private volatile IRetryPolicy _retryPolicy;

        /// <summary>
        ///  Gets the consistency level.
        /// </summary>
        public ConsistencyLevel ConsistencyLevel { get { return _consistency; } }

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
            this._consistency = ConsistencyLevel.Default;
        }

        /// <summary>
        ///  Sets the consistency level for the query. <p> The default consistency level,
        ///  if this method is not called, is ConsistencyLevel.ONE.</p>
        /// </summary>
        /// <param name="consistency"> the consistency level to set. </param>
        /// 
        /// <returns>this <code>Query</code> object.</returns>
        public Query SetConsistencyLevel(ConsistencyLevel consistency)
        {
            this._consistency = consistency;
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

        protected internal abstract IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state);

        protected internal abstract RowSet EndSessionExecute(Session session, IAsyncResult ar);

    }
}
