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
