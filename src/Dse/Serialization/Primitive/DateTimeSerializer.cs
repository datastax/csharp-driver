//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Serialization.Primitive
{
    internal class DateTimeSerializer : TypeSerializer<DateTime>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Timestamp; }
        }

        public override DateTime Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var dto = DateTimeOffsetSerializer.Deserialize(buffer, offset);
            return dto.DateTime;
        }

        public override byte[] Serialize(ushort protocolVersion, DateTime value)
        {
            // Treat "Unspecified" as UTC (+0) not the default behavior of DateTimeOffset which treats as Local Timezone
            // because we are about to do math against EPOCH which must align with UTC.
            var dateTimeOffset = value.Kind == DateTimeKind.Unspecified
                ? new DateTimeOffset(value, TimeSpan.Zero)
                : new DateTimeOffset(value);
            return DateTimeOffsetSerializer.Serialize(dateTimeOffset);
        }
    }
}
