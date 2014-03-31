namespace Cassandra
{
    public class SchemaChangeEventArgs:CassandraEventArgs
    {
        public enum Reason
        {
            Created,
            Updated,
            Dropped
        };
        public Reason What;
        public string Keyspace;
        public string Table;
    }
}