namespace Cassandra
{
    /// <summary>
    /// Exception thrown when a query attemps to create a keyspace or table that already exists.
    /// </summary>
    public class AlreadyExistsException : QueryValidationException
    {
        public string Keyspace { get; private set; }
        public string Table { get; private set; }

        public AlreadyExistsException(string Message, string Ks, string Table) :
            base(Message) { this.Keyspace = Ks; this.Table = Table; }
     
    }
}