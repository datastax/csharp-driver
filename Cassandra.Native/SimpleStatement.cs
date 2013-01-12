using Cassandra;
using System;
namespace Cassandra
{

    /**
     * A simple {@code Statement} implementation built directly from a query
     * string.
     */
    public class SimpleStatement : Statement
    {

        private readonly string query;
        private volatile CassandraRoutingKey routingKey;

        /**
         * Creates a new {@code SimpleStatement} with the provided query string.
         *
         * @param query the query string.
         */
        public SimpleStatement(string query)
        {
            this.query = query;
        }

        /**
         * The query string.
         *
         * @return the query string;
         */
        public override string QueryString { get { return query; } }

        /**
         * The routing key for the query.
         * <p>
         * Note that unless the routing key has been explicitly set through
         * {@link #setRoutingKey}, this will method will return {@code null} (to
         * avoid having to parse the query string to retrieve the partition key).
         *
         * @return the routing key set through {@link #setRoutingKey} is such a key
         * was set, {@code null} otherwise.
         *
         * @see Query#getRoutingKey
         */
        public override CassandraRoutingKey RoutingKey { get { return routingKey; } }
        public SimpleStatement SetRoutingKey(params CassandraRoutingKey[] routingKeys) 
        {
            this.routingKey = CassandraRoutingKey.Compose(routingKeys); return this; 
        }


        internal override IAsyncResult BeginExecute(Session session, AsyncCallback callback, object state)
        {
            return session.BeginQuery(QueryString, callback, state, ConsistencyLevel, RoutingKey, this);
        }

        internal override CqlRowSet EndExecute(Session session, IAsyncResult ar)
        {
            return session.EndQuery(ar);
        }
    }
}