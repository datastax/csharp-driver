//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra.Serialization.Primitive
{
    internal class TimeUuidSerializer : TypeSerializer<TimeUuid>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Timeuuid; }
        }

        public override TimeUuid Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return new Guid(GuidShuffle(buffer, offset));
        }

        public override byte[] Serialize(ushort protocolVersion, TimeUuid value)
        {
            return GuidShuffle(value.ToByteArray());
        }
    }
}
