
namespace Cassandra
{

    /// <summary>
    ///  Represents a prepared statement, a query with bound variables that has been
    ///  prepared (pre-parsed) by the database. <p> A prepared statement can be
    ///  executed once concrete values has been provided for the bound variables. The
    ///  pair of a prepared statement and values for its bound variables is a
    ///  BoundStatement and can be executed (by <link>Session#Execute</link>).</p>
    /// </summary>
    public class PreparedStatement
    {
        private volatile ConsistencyLevel _consistency;
        private volatile CassandraRoutingKey _routingKey;

        internal readonly RowSetMetadata Metadata;
        internal readonly byte[] Id;

        internal PreparedStatement(RowSetMetadata metadata, byte[] id)
        {
            this.Metadata = metadata;
            this.Id = id;
        }

        /// <summary>
        ///  Gets metadata on the bounded variables of this prepared statement.
        /// </summary>
        public RowSetMetadata Variables { get { return Metadata; } }

        /// <summary>
        ///  Sets a default consistency level for all <code>BoundStatement</code> created
        ///  from this object. <p> If no consistency level is set through this method, the
        ///  BoundStatement created from this object will use the default consistency
        ///  level (One). </p><p> Changing the default consistency level is not retroactive,
        ///  it only applies to BoundStatement created after the change.</p>
        /// </summary>
        /// <param name="consistency"> the default consistency level to set. </param>
        /// 
        /// <returns>this <code>PreparedStatement</code> object.</returns>
        public PreparedStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            this._consistency = consistency;
            return this;
        }

        public CassandraRoutingKey RoutingKey { get { return _routingKey; } }

        /// <summary>
        ///  Set the routing key for this query. <p> See
        ///  <link>#setRoutingKey(ByteBuffer)</link> for more information. This method is
        ///  a variant for when the query partition key is composite and thus the routing
        ///  key must be built from multiple values.</p>
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose to
        ///  obtain the routing key. </param>
        /// 
        /// <returns>this <code>PreparedStatement</code> object.
        ///  <see>Query#GetRoutingKey</returns>
        public PreparedStatement SetRoutingKey(params CassandraRoutingKey[] routingKeyComponents)
        {
            this._routingKey = CassandraRoutingKey.Compose(routingKeyComponents); return this;
        }

        /// <summary>
        ///  Creates a new BoundStatement object and bind its variables to the provided
        ///  values. <p> This method is a shortcut for <code>new
        ///  BoundStatement(this).Bind(...)</code>. <p> Note that while no more
        ///  <code>values</code> than bound variables can be provided, it is allowed to
        ///  provide less <code>values</code> that there is variables. In that case, the
        ///  remaining variables will have to be bound to values by another mean because
        ///  the resulting <code>BoundStatement</code> being executable.</p>
        /// </summary>
        /// <param name="values"> the values to bind to the variables of the newly
        ///  created BoundStatement. </param>
        /// 
        /// <returns>the newly created <code>BoundStatement</code> with its variables
        ///  bound to <code>values</code>. </returns>
        public BoundStatement Bind(params object[] values)
        {
            var bs = new BoundStatement(this);
            return bs.Bind(values);
        }
    }
}