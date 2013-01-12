using Cassandra.Native;
namespace Cassandra
{

    /**
     * Represents a prepared statement, a query with bound variables that has been
     * prepared (pre-parsed) by the database.
     * <p>
     * A prepared statement can be executed once concrete values has been provided
     * for the bound variables. The pair of a prepared statement and values for its
     * bound variables is a BoundStatement and can be executed (by
     * {@link Session#execute}).
     */
    public class PreparedStatement
    {
        private volatile ConsistencyLevel consistency;
        private volatile CassandraRoutingKey routingKey;

        internal readonly TableMetadata metadata;
        internal readonly byte[] id;

        internal PreparedStatement(TableMetadata metadata, byte[] id)
        {
            this.metadata = metadata;
            this.id = id;
        }

        /**
 * Returns metadata on the bounded variables of this prepared statement.
 *
 * @return the variables bounded in this prepared statement.
 */
        public TableMetadata Variables { get { return metadata; } }

        /**
         * Sets the consistency level for the query.
         * <p>
         * The default consistency level, if this method is not called, is ConsistencyLevel.ONE.
         *
         * @param consistency the consistency level to set.
         * @return this {@code Query} object.
         */
        public PreparedStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            this.consistency = consistency;
            return this;
        }

        public CassandraRoutingKey RoutingKey { get { return routingKey; } }
        public PreparedStatement SetRoutingKey(params CassandraRoutingKey[] routingKeys)
        {
            this.routingKey = CassandraRoutingKey.Compose(routingKeys); return this;
        }
        /**
         * Creates a new BoundStatement object and bind its variables to the
         * provided values.
         * <p>
         * This method is a shortcut for {@code new BoundStatement(this).bind(...)}.
         * <p>
         * Note that while no more {@code values} than bound variables can be
         * provided, it is allowed to provide less {@code values} that there is
         * variables. In that case, the remaining variables will have to be bound
         * to values by another mean because the resulting {@code BoundStatement}
         * being executable.
         *
         * @param values the values to bind to the variables of the newly created
         * BoundStatement.
         * @return the newly created {@code BoundStatement} with its variables
         * bound to {@code values}.
         *
         * @throws IllegalArgumentException if more {@code values} are provided
         * than there is of bound variables in this statement.
         * @throws InvalidTypeException if any of the provided value is not of
         * correct type to be bound to the corresponding bind variable.
         *
         * @see BoundStatement#bind
         */
        public BoundStatement Bind(params object[] values)
        {
            BoundStatement bs = new BoundStatement(this);
            return bs.Bind(values);
        }
    }
}