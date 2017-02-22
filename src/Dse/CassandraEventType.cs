//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

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