using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public class CassandraRoutingKey
    {
        public static CassandraRoutingKey Empty = new CassandraRoutingKey();
        public byte[] rawRoutingKey = null;
        public static CassandraRoutingKey Compose(params CassandraRoutingKey[] components)
        {
            return null;
        }
    }
}
