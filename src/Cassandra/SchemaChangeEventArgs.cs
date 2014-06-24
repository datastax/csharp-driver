namespace Cassandra
{
    internal class SchemaChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            Created,
            Updated,
            Dropped
        };

        /// <summary>
        /// The keyspace affected
        /// </summary>
        public string Keyspace { get; set; }
        /// <summary>
        /// The table affected
        /// </summary>
        public string Table { get; set; }
        public Reason What { get; set; }
        /// <summary>
        /// The custom type affected
        /// </summary>
        public string Type { get; set; }
    }
}