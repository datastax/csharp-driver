﻿//
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
    internal class BooleanSerializer : TypeSerializer<bool>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Boolean; }
        }

        public override bool Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return buffer[offset] == 1;
        }

        public override byte[] Serialize(ushort protocolVersion, bool value)
        {
            return new [] { (byte)(value ? 1 : 0) };
        }
    }
}
