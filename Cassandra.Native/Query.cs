using System;

namespace Cassandra
{
    /**
     * An executable query.
     * <p>
     * This represents either a {@link Statement} or a {@link BoundStatement}
     * along with the query options (consistency level, whether to trace the query, ...).
     */
    public abstract class Query
    {
        private volatile ConsistencyLevel _consistency;
        private volatile bool _traceQuery;

        private volatile RetryPolicy _retryPolicy;

        public ConsistencyLevel ConsistencyLevel { get { return _consistency; } }
        public bool IsTracing { get { return _traceQuery; } }

        public RetryPolicy RetryPolicy { get { return _retryPolicy; } }

        // We don't want to expose the constructor, because the code rely on this being only subclassed by Statement and BoundStatement
        protected Query()
        {
            this._consistency = ConsistencyLevel.ONE;
        }
        
        /**
         * Sets the consistency level for the query.
         * <p>
         * The default consistency level, if this method is not called, is ConsistencyLevel.ONE.
         *
         * @param consistency the consistency level to set.
         * @return this {@code Query} object.
         */
        public Query SetConsistencyLevel(ConsistencyLevel consistency)
        {
            this._consistency = consistency;
            return this;
        }

        /**
         * Enable tracing for this query.
         *
         * By default (i.e. unless you call this method), tracing is not enabled.
         *
         * @return this {@code Query} object.
         */
        public Query EnableTracing()
        {
            this._traceQuery = true;
            return this;
        }

        /**
         * Disable tracing for this query.
         *
         * @return this {@code Query} object.
         */
        public Query DisableTracing()
        {
            this._traceQuery = false;
            return this;
        }
        
        /**
         * The routing key (in binary raw form) to use for token aware routing of this query.
         * <p>
         * The routing key is optional in the sense that implementers are free to
         * return {@code null}. The routing key is an hint used for token aware routing (see
         * {@link com.datastax.driver.core.policies.TokenAwarePolicy}), and
         * if provided should correspond to the binary value for the query
         * partition key. However, not providing a routing key never causes a query
         * to fail and if the load balancing policy used is not token aware, then
         * the routing key can be safely ignored.
         *
         * @return the routing key for this query or {@code null}.
         */
        public abstract CassandraRoutingKey RoutingKey { get; }


        /**
         * Sets the retry policy to use for this query.
         * <p>
         * The default retry policy, if this method is not called, is the one returned by
         * {@link com.datastax.driver.core.policies.Policies#getRetryPolicy} in the
         * cluster configuration. This method is thus only useful in case you want
         * to punctually override the default policy for this request.
         *
         * @param policy the retry policy to use for this query.
         * @return this {@code Query} object.
         */
        public Query SetRetryPolicy(RetryPolicy policy)
        {
            this._retryPolicy = policy;
            return this;
        }
        internal abstract IAsyncResult BeginExecute(Session session, AsyncCallback callback, object state);

        internal abstract CqlRowSet EndExecute(Session session, IAsyncResult ar);

    }
}
