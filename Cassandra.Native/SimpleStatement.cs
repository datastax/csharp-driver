using System;

namespace Cassandra
{

    /// <summary>
    ///  A simple <code>Statement</code> implementation built directly from a query
    ///  string.
    /// </summary>
    public class SimpleStatement : Statement
    {

        private readonly string _query;
        private volatile CassandraRoutingKey _routingKey;

        /// <summary>
        ///  Creates a new <code>SimpleStatement</code> with the provided query string.
        /// </summary>
        /// <param name="query"> the query string.</param>
        public SimpleStatement(string query)
        {
            this._query = query;
        }

        /// <summary>
        ///  Gets the query string.
        /// </summary>
        public override string QueryString { get { return _query; } }

        /// <summary>
        ///  Gets the routing key for the query. <p> Note that unless the routing key has been
        ///  explicitly set through <link>#setRoutingKey</link>, this will method will
        ///  return <code>null</code> (to avoid having to parse the query string to
        ///  retrieve the partition key).
        /// </summary>
        public override CassandraRoutingKey RoutingKey { get { return _routingKey; } }

        /// <summary>
        ///  Set the routing key for this query. <p> This method allows to manually
        ///  provide a routing key for this query. It is thus optional since the routing
        ///  key is only an hint for token aware load balancing policy but is never
        ///  mandatory. <p> If the partition key for the query is composite, use the
        ///  <link>#setRoutingKey(ByteBuffer...)</link> method instead to build the
        ///  routing key.
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose to
        ///  obtain the routing key.
        ///  </param>
        /// 
        /// <returns>this <code>SimpleStatement</code> object.
        ///  <see>Query#getRoutingKey</returns>
        public SimpleStatement SetRoutingKey(params CassandraRoutingKey[] routingKeyComponents) 
        {
            this._routingKey = CassandraRoutingKey.Compose(routingKeyComponents); return this; 
        }


        internal override IAsyncResult BeginExecute(Session session, AsyncCallback callback, object state)
        {
            return session.BeginQuery(QueryString, callback, state, ConsistencyLevel, this, this);
        }

        internal override CqlRowSet EndExecute(Session session, IAsyncResult ar)
        {
            return session.EndQuery(ar);
        }
    }
}