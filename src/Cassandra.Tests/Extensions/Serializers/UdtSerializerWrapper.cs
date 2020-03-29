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

using System.Text;
using Cassandra.Serialization;

namespace Cassandra.Tests.Extensions.Serializers
{
    public class UdtSerializerWrapper : UdtSerializer
    {
        private readonly bool _fixedValue;
        public int SerializationCounter { get; private set; }

        public int DeserializationCounter { get; private set; }

        public UdtSerializerWrapper(bool fixedValue = true)
        {
            _fixedValue = fixedValue;
        }

        public override byte[] Serialize(ushort protocolVersion, object value)
        {
            SerializationCounter++;
            if (_fixedValue)
            {
                return Encoding.UTF8.GetBytes("DUMMY UDT SERIALIZED");   
            }
            return base.Serialize(protocolVersion, value);
        }

        public override object Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            DeserializationCounter++;
            if (_fixedValue)
            {
                return Utils.SliceBuffer(buffer, offset, length);   
            }
            return base.Deserialize(protocolVersion, buffer, offset, length, typeInfo);
        }
    }
}
