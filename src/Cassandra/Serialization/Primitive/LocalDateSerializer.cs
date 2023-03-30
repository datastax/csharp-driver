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

namespace Cassandra.Serialization.Primitive
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
