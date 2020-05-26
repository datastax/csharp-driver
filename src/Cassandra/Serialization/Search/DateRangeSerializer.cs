//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using Cassandra.DataStax.Search;

namespace Cassandra.Serialization.Search
{
    internal class DateRangeSerializer : TypeSerializer<DateRange>
    {
        private readonly IColumnInfo _typeInfo = new CustomColumnInfo("org.apache.cassandra.db.marshal.DateRangeType");
        
        /// <summary>
        /// The byte length of the serialized DateRange with a single boundary: byte + long + byte
        /// </summary>
        private const int ByteLengthSingleBoundary = 10;

        /// <summary>
        /// The byte length of the serialized DateRange with a 2 boundaries: byte + long + byte + long + byte
        /// </summary>
        private const int ByteLengthTwoBoundaries = 19;

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return _typeInfo; }
        }

        public override DateRange Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            if (length == 0)
            {
                throw new ArgumentException("DateRange serialized value must have at least 1 byte");
            }
            var type = (RangeType) buffer[offset++];
            switch (type)
            {
                case RangeType.SingleValue:
                    return new DateRange(ReadDateRangeBound(buffer, offset));
                case RangeType.ClosedRange:
                    return new DateRange(ReadDateRangeBound(buffer, offset), ReadDateRangeBound(buffer, offset + 9));
                case RangeType.OpenRangeHigh:
                    return new DateRange(ReadDateRangeBound(buffer, offset), DateRangeBound.Unbounded);
                case RangeType.OpenRangeLow:
                    return new DateRange(DateRangeBound.Unbounded, ReadDateRangeBound(buffer, offset));
                case RangeType.OpenBoth:
                    return new DateRange(DateRangeBound.Unbounded, DateRangeBound.Unbounded);
                case RangeType.OpenSingle:
                    return new DateRange(DateRangeBound.Unbounded);
            }
            throw new ArgumentException(string.Format("Range type {0} not supported", type));
        }

        private DateRangeBound ReadDateRangeBound(byte[] buffer, int offset)
        {
            var millis = EndianBitConverter.ToInt64(false, buffer, offset);
            return new DateRangeBound(UnixStart.AddMilliseconds(millis), (DateRangePrecision) buffer[offset + 8]);
        }

        public override byte[] Serialize(ushort protocolVersion, DateRange value)
        {
            // Serializes the value containing:
            // <type>[<time0><precision0><time1><precision1>]
            byte[] buffer;
            if (value.LowerBound == DateRangeBound.Unbounded)
            {
                if (value.UpperBound == null)
                {
                    return new[] { (byte)RangeType.OpenSingle };
                }
                if (value.UpperBound == DateRangeBound.Unbounded)
                {
                    return new[] { (byte)RangeType.OpenBoth };
                }
                // byte + long + byte
                buffer = new byte[10];
                buffer[0] = (byte) RangeType.OpenRangeLow;
                WriteDateRangeBound(buffer, 1, value.UpperBound.Value);
                return buffer;
            }
            if (value.UpperBound == null)
            {
                buffer = new byte[ByteLengthSingleBoundary];
                buffer[0] = (byte)RangeType.SingleValue;
                WriteDateRangeBound(buffer, 1, value.LowerBound);
                return buffer;
            }
            if (value.UpperBound == DateRangeBound.Unbounded)
            {
                buffer = new byte[ByteLengthSingleBoundary];
                buffer[0] = (byte)RangeType.OpenRangeHigh;
                WriteDateRangeBound(buffer, 1, value.LowerBound);
                return buffer;
            }
            var offset = 0;
            buffer = new byte[ByteLengthTwoBoundaries];
            buffer[offset++] = (byte)RangeType.ClosedRange;
            offset = WriteDateRangeBound(buffer, offset, value.LowerBound);
            WriteDateRangeBound(buffer, offset, value.UpperBound.Value);
            return buffer;
        }

        private static int WriteDateRangeBound(byte[] buffer, int offset, DateRangeBound value)
        {
            var ticksDiff = value.Timestamp.Ticks - UnixStart.Ticks;
            var millisecondsDiff = ticksDiff / (decimal)TimeSpan.TicksPerMillisecond;
            var millis = Convert.ToInt64(Math.Floor(millisecondsDiff));
            EndianBitConverter.SetBytes(false, buffer, offset, millis);
            buffer[offset + 8] = (byte) value.Precision;
            return offset + 9;
        }

        private enum RangeType : byte
        {
          // single value as in "2001-01-01"
          SingleValue = 0,
          // closed range as in "[2001-01-01 TO 2001-01-31]"
          ClosedRange = 1,
          // open range high as in "[2001-01-01 TO *]"
          OpenRangeHigh = 2,
          // - 0x03 - open range low as in "[* TO 2001-01-01]"
          OpenRangeLow = 3,
          // - 0x04 - both ranges open as in "[* TO *]"
          OpenBoth = 4,
          // - 0x05 - single open range as in "[*]"
          OpenSingle = 5
        }
    }
}
