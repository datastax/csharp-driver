namespace Cassandra
{
    public class CassandraRoutingKey
    {
        public static CassandraRoutingKey Empty = new CassandraRoutingKey();
        public byte[] RawRoutingKey = null;
        public static CassandraRoutingKey Compose(params CassandraRoutingKey[] components)
        {
            return null;
        }
    }
}
