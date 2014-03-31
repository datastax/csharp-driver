namespace Cassandra
{
    public class TableColumn : CqlColumn
    {
        public KeyType KeyType;
        public string SecondaryIndexName;
        public string SecondaryIndexType;
    }
}