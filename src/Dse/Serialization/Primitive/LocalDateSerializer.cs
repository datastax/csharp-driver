//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Serialization.Primitive
{
    internal class LocalDateSerializer : TypeSerializer<LocalDate>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Date; }
        }

        public override LocalDate Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var days = unchecked((uint)((buffer[offset] << 24)
                   | (buffer[offset + 1] << 16)
                   | (buffer[offset + 2] << 8)
                   | (buffer[offset + 3])));
            return new LocalDate(days);
        }

        public override byte[] Serialize(ushort protocolVersion, LocalDate value)
        {
            var val = value.DaysSinceEpochCentered;
            return new[]
            {
                (byte) ((val & 0xFF000000) >> 24),
                (byte) ((val & 0xFF0000) >> 16),
                (byte) ((val & 0xFF00) >> 8),
                (byte) (val & 0xFF)
            };
        }
    }
}
