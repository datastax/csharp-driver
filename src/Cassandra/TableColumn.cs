namespace Cassandra
{
    /// <summary>
    /// Represents a table column information
    /// </summary>
    public class TableColumn : CqlColumn
    {
        public KeyType KeyType { get; set; }
        public string SecondaryIndexName { get; set; }
        public string SecondaryIndexType { get; set; }
    }
}