namespace Cassandra
{
    public class HostShard
    {
        public Host Host { get; }
        public int Shard { get; }

        public HostShard(Host host, int shard)
        {
            Host = host;
            Shard = shard;
        }

        public override string ToString() => $"HostShard {{host={Host.Address}, shard={Shard}}}";
    }
}