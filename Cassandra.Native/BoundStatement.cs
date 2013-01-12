using Cassandra;
using System;
namespace Cassandra
{
    /**
     * A prepared statement with values bound to the bind variables.
     * <p>
     * Once a BoundStatement has values for all the variables of the {@link PreparedStatement}
     * it has been created from, it can executed (through {@link Session#execute}).
     * <p>
     * The values of a BoundStatement can be set by either index or name. When
     * setting them by name, names follow the case insensitivity rules explained in
     * {@link ColumnDefinitions}. Noteworthily, if multiple bind variables
     * correspond to the same column (as would be the case if you prepare
     * {@code SELECT * FROM t WHERE x > ? AND x < ?}), you will have to set
     * values by indexes (or the {@link #bind} method) as the methods to set by
     * name only allows to set the first prepared occurrence of the column.
     */
    public class BoundStatement : Query
    {

        readonly PreparedStatement _statement;

        object[] _values;

        /**
         * Creates a new {@code BoundStatement} from the provided prepared
         * statement.
         *
         * @param statement the prepared statement from which to create a t {@code BoundStatement}.
         */
        public BoundStatement(PreparedStatement statement)
        {
            this._statement = statement;
        }

        /**
         * Returns the prepared statement on which this BoundStatement is based.
         *
         * @return the prepared statement on which this BoundStatement is based.
         */
        public PreparedStatement PreparedStatement()
        {
            return _statement;
        }

        /**
         * Bound values to the variables of this statement.
         *
         * This method provides a convenience to bound all the variables of the
         * {@code BoundStatement} in one call.
         *
         * @param values the values to bind to the variables of the newly created
         * BoundStatement. The first element of {@code values} will be bound to the
         * first bind variable, etc.. It is legal to provide less values than the
         * statement has bound variables. In that case, the remaining variable need
         * to be bound before execution. If more values than variables are provided
         * however, an IllegalArgumentException wil be raised.
         * @return this bound statement.
         *
         * @throws IllegalArgumentException if more {@code values} are provided
         * than there is of bound variables in this statement.
         * @throws InvalidTypeException if any of the provided value is not of
         * correct type to be bound to the corresponding bind variable.
         */
        public BoundStatement Bind(params object[] values)
        {
            this._values = values;
            return this;
        }

        /**
         * The routing key for this bound query.
         * <p>
         * This method will return a non-{@code null} value if:
         * <ul>
         *   <li>either all the columns composing the partition key are bound
         *   variables of this {@code BoundStatement}. The routing key will then be
         *   built using the values provided for these partition key columns.</li>
         *   <li>or the routing key has been set through {@link PreparedStatement#setRoutingKey}
         *   for the {@code PreparedStatement} this statement has been built from.</li>
         * </ul>
         * Otherwise, {@code null} is returned.
         * <p>
         * Note that if the routing key has been set through {@link PreparedStatement#setRoutingKey},
         * that value takes precedence even if the partition key is part of the bound variables.
         *
         * @return the routing key for this statement or {@code null}.
         */
        public override CassandraRoutingKey RoutingKey { get { return null; } }

        internal override IAsyncResult BeginExecute(Session session, AsyncCallback callback, object state)
        {
            return session.BeginExecuteQuery(PreparedStatement().Id, PreparedStatement().Metadata, _values, callback, state, ConsistencyLevel, RoutingKey, this);
        }

        internal override CqlRowSet EndExecute(Session session, IAsyncResult ar)
        {
            return session.EndExecuteQuery(ar);
        }
    }
}