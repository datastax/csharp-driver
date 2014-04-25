namespace TPLSample.LinqKeyspacesSample
{
    public class SchemaColumns
    {
        public string keyspace_name { get; set; }

        public string columnfamily_name { get; set; }

        public string column_name { get; set; }

        public int component_index { get; set; }

        public string validator { get; set; }
    }
}