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
    internal class ShortSerializer : TypeSerializer<short>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.SmallInt; }
        }

        public override short Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return BeConverter.ToInt16(buffer, offset);
        }

        public override byte[] Serialize(ushort protocolVersion, short value)
        {
            return BeConverter.GetBytes(value);
        }
    }
}
