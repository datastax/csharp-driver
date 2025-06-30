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

        public override bool Equals(object obj)
        {
            if (obj is HostShard other)
            {
                return Host.Equals(other.Host) && Shard == other.Shard;
            }
            return false;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 23 + (Host != null ? Host.GetHashCode() : 0);
                hash = hash * 23 + Shard.GetHashCode();
                return hash;
            }
        }
    }
}