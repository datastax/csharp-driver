namespace TPLSample.FutureSample
{
    public class schema_keyspaces
    {
        public bool durable_writes { get; set; }

        public string keyspace_name { get; set; }

        public string strategy_class { get; set; }

        public string strategy_options { get; set; }
    }
}