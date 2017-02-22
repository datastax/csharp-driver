//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra.Serialization.Primitive
{
    internal class DateTimeOffsetSerializer : TypeSerializer<DateTimeOffset>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Timestamp; }
        }

        internal static DateTimeOffset Deserialize(byte[] buffer, int offset)
        {
            return UnixStart.AddMilliseconds(BeConverter.ToInt64(buffer, offset));
        }

        internal static byte[] Serialize(DateTimeOffset value)
        {
            return BeConverter.GetBytes(Convert.ToInt64(Math.Floor((value - UnixStart).TotalMilliseconds)));
        }

        public override DateTimeOffset Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return Deserialize(buffer, offset);
        }

        public override byte[] Serialize(ushort protocolVersion, DateTimeOffset value)
        {
            return Serialize(value);
        }
    }
}
