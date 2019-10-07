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
using System.Numerics;

namespace Cassandra.Serialization.Primitive
{
    /// <summary>
    /// A serializer for CQL type varint, CLR type BigInteger.
    /// </summary>
    internal class BigIntegerSerializer : TypeSerializer<BigInteger>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Varint; }
        }

        public override BigInteger Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            buffer = Utils.SliceBuffer(buffer, offset, length);
            //Cassandra uses big endian encoding
            Array.Reverse(buffer);
            return new BigInteger(buffer);
        }

        public override byte[] Serialize(ushort protocolVersion, BigInteger value)
        {
            var buffer = value.ToByteArray();
            //Cassandra expects big endian encoding
            Array.Reverse(buffer);
            return buffer;
        }
    }
}
