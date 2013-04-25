namespace Cassandra
{
    /// <summary>
    ///  Exception thrown when a query attemps to create a keyspace or table that
    ///  already exists.
    /// </summary>
    public class AlreadyExistsException : QueryValidationException
    {
        /// <summary>
        ///  Gets the name of keyspace that either already exists or is home to the table that
        ///  already exists.
        /// </summary>
        public string Keyspace { get; private set; }

        /// <summary>
        ///  If the failed creation was a table creation, gets the name of the table that already exists. 
        /// </summary>
        public string Table { get; private set; }

        /// <summary>
        ///  Gets whether the query yielding this exception was a table creation
        ///  attempt.
        /// </summary>
        public bool WasTableCreation { get { return !string.IsNullOrEmpty((Table)); } }

        public AlreadyExistsException(string keyspace, string table) :
            base(makeMsg(keyspace, table))
        {
            this.Keyspace = string.IsNullOrWhiteSpace(keyspace) ? null : keyspace;
            this.Table = string.IsNullOrWhiteSpace(table) ? null : table;
        }

        private static string makeMsg(string keyspace, string table)
        {
            if (string.IsNullOrEmpty(table))
                return string.Format("Keyspace {0} already exists", keyspace);
            else
                return string.Format("Table {0}.{1} already exists", keyspace, table);
        }
    }
}