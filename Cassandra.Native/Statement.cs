namespace Cassandra
{
    /// <summary>
    ///  A non-prepared CQL statement. <p> This class represents a query string along
    ///  with query options. This class can be extended but
    ///  <link>SimpleStatement</link> is provided to build a <code>* Statement</code>
    ///  directly from its query string.
    /// </summary>
    public abstract class Statement : Query
    {

        /// <summary>
        ///  Gets the query string for this statement.
        /// </summary>
        public abstract string QueryString { get; }

        public override string ToString()
        {
            return QueryString;
        }
    }
}