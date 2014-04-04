namespace Cassandra
{
    public class SchemaChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            Created,
            Updated,
            Dropped
        };

        public string Keyspace;
        public string Table;
        public Reason What;
    }
}