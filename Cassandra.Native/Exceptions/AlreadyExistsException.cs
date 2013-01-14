namespace Cassandra
{
    /// <summary>
    ///  Exception thrown when a query attemps to create a keyspace or table that already exists.
    /// </summary>
    public class AlreadyExistsException : QueryValidationException
    {
        /// <summary>
        ///  Gets the name of keyspace that either already exists or is home to the table that already exists. 
        /// </summary>
        public string Keyspace { get; private set; }

        /// <summary>
        ///  If the failed creation was a table creation, gets the name of the table that already exists. 
        /// </summary>
        public string Table { get; private set; }

        /// <summary>
        ///  Gets whether the query yielding this exception was a table creation attempt. 
        /// </summary>
        public bool WasTableCreation { get { return !string.IsNullOrEmpty((Table)); } }

        public AlreadyExistsException(string message, string keyspace, string table) :
            base(message)
        {
            this.Keyspace = keyspace;
            this.Table = table;
        }

    }
}