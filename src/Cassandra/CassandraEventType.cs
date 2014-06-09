using System;

namespace Cassandra
{
    [Flags]
    internal enum CassandraEventType
    {
        TopologyChange = 0x01,
        StatusChange = 0x02,
        SchemaChange = 0x03
    }
}