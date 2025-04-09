namespace Cassandra
{
    /// <summary>
    /// Represents Scylla connection options as sent in SUPPORTED
    /// frame.
    /// </summary>
    public class ShardingInfo
    {
        public int ScyllaShard { get; }
        public int ScyllaNrShards { get; }
        public string ScyllaPartitioner { get; }
        public string ScyllaShardingAlgorithm { get; }
        public ulong ScyllaShardingIgnoreMSB { get; }
        public ulong ScyllaShardAwarePort { get; }
        public ulong ScyllaShardAwarePortSSL { get; }

        private ShardingInfo(int scyllaShard, int scyllaNrShards, string scyllaPartitioner,
                         string scyllaShardingAlgorithm, ulong scyllaShardingIgnoreMSB,
                         ulong scyllaShardAwarePort, ulong scyllaShardAwarePortSSL)
        {
            ScyllaShard = scyllaShard;
            ScyllaNrShards = scyllaNrShards;
            ScyllaPartitioner = scyllaPartitioner;
            ScyllaShardingAlgorithm = scyllaShardingAlgorithm;
            ScyllaShardingIgnoreMSB = scyllaShardingIgnoreMSB;
            ScyllaShardAwarePort = scyllaShardAwarePort;
            ScyllaShardAwarePortSSL = scyllaShardAwarePortSSL;
        }

        public static ShardingInfo Create(string scyllaShard, string scyllaNrShards, string scyllaPartitioner,
                                        string scyllaShardingAlgorithm, string scyllaShardingIgnoreMSB,
                                        string scyllaShardAwarePort, string scyllaShardAwarePortSSL)
        {
            return new ShardingInfo(
                int.Parse(scyllaShard),
                int.Parse(scyllaNrShards),
                scyllaPartitioner,
                scyllaShardingAlgorithm,
                ulong.Parse(scyllaShardingIgnoreMSB),
                ulong.Parse(scyllaShardAwarePort),
                ulong.Parse(scyllaShardAwarePortSSL)
            );
        }
    }
}