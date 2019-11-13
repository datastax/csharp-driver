//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra.Serialization
{
    internal interface ITypeSerializer
    {
        Type Type { get; }

        IColumnInfo TypeInfo { get; }

        ColumnTypeCode CqlType { get; }

        object Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo);

        byte[] Serialize(ushort protocolVersion, object obj);
    }
}
