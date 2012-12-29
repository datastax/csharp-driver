namespace Cassandra
{
    /**
     * A non-prepared CQL statement.
     * <p>
     * This class represents a query string along with query options. This class
     * can be extended but {@link SimpleStatement} is provided to build a {@code
     * Statement} directly from its query string.
     */
    public abstract class Statement : Query
    {

        /**
         * The query string for this statement.
         *
         * @return a valid CQL query string.
         */
        public abstract string QueryString { get; }

        public override string ToString()
        {
            return QueryString;
        }
    }
}