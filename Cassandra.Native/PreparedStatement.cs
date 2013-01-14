
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
        private volatile ConsistencyLevel _consistency;
        private volatile CassandraRoutingKey _routingKey;

        internal readonly TableMetadata Metadata;
        internal readonly byte[] Id;

        internal PreparedStatement(TableMetadata metadata, byte[] id)
        {
            this.Metadata = metadata;
            this.Id = id;
        }

        /**
 * Returns metadata on the bounded variables of this prepared statement.
 *
 * @return the variables bounded in this prepared statement.
 */
        public TableMetadata Variables { get { return Metadata; } }

        /**
         * Sets the consistency level for the query.
         * <p>
         * The default consistency level, if this method is not called, is Consistency.ONE.
         *
         * @param consistency the consistency level to set.
         * @return this {@code Query} object.
         */
        public PreparedStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            this._consistency = consistency;
            return this;
        }

        public CassandraRoutingKey RoutingKey { get { return _routingKey; } }
        public PreparedStatement SetRoutingKey(params CassandraRoutingKey[] routingKeys)
        {
            this._routingKey = CassandraRoutingKey.Compose(routingKeys); return this;
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
            var bs = new BoundStatement(this);
            return bs.Bind(values);
        }
    }
}