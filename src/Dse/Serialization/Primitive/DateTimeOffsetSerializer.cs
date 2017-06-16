//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Serialization.Primitive
{
    internal class DateTimeOffsetSerializer : TypeSerializer<DateTimeOffset>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Timestamp; }
        }

        internal static DateTimeOffset Deserialize(byte[] buffer, int offset)
        {
            var milliseconds = BeConverter.ToInt64(buffer, offset);
            return UnixStart.AddTicks(TimeSpan.TicksPerMillisecond * milliseconds);
        }

        internal static byte[] Serialize(DateTimeOffset value)
        {
            var ticks = (value - UnixStart).Ticks;
            return BeConverter.GetBytes(ticks / TimeSpan.TicksPerMillisecond);
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
