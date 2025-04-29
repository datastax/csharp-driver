namespace Cassandra.Connections
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
        public long ScyllaShardingIgnoreMSB { get; }
        public int ScyllaShardAwarePort { get; }
        public int ScyllaShardAwarePortSSL { get; }

        private ShardingInfo(int scyllaShard, int scyllaNrShards, string scyllaPartitioner,
                         string scyllaShardingAlgorithm, long scyllaShardingIgnoreMSB,
                         int scyllaShardAwarePort, int scyllaShardAwarePortSSL)
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
                long.Parse(scyllaShardingIgnoreMSB),
                int.Parse(scyllaShardAwarePort),
                int.Parse(scyllaShardAwarePortSSL)
            );
        }

        internal int ShardID(IToken t)
        {
            long token = long.Parse(t.ToString());
            token += long.MinValue;
            token <<= (int)ScyllaShardingIgnoreMSB;

            ulong tokLo = (ulong)(token & 0xFFFFFFFFL);
            ulong tokHi = (ulong)((token >> 32) & 0xFFFFFFFFL);

            ulong mul1 = tokLo * (ulong)ScyllaNrShards;
            ulong mul2 = tokHi * (ulong)ScyllaNrShards; // logically shifted 32 bits

            ulong sum = (mul1 >> 32) + mul2;

            return (int)(sum >> 32);
        }


        public override string ToString()
        {
            return $"ShardingInfo: " +
                $"ScyllaShard={ScyllaShard}, " +
                $"ScyllaNrShards={ScyllaNrShards}, " +
                $"ScyllaPartitioner={ScyllaPartitioner}, " +
                $"ScyllaShardingAlgorithm={ScyllaShardingAlgorithm}, " +
                $"ScyllaShardingIgnoreMSB={ScyllaShardingIgnoreMSB}, " +
                $"ScyllaShardAwarePort={ScyllaShardAwarePort}, " +
                $"ScyllaShardAwarePortSSL={ScyllaShardAwarePortSSL}";
        }
    }
}
