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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Serialization;

namespace Cassandra.Tests.Extensions.Serializers
{
    public class DummyCustomTypeSerializer : CustomTypeSerializer<DummyCustomType>
    {
        public DummyCustomTypeSerializer(string name = "ORG.DUMMY.CUSTOM.TYPE")
            : base(name)
        {

        }

        public override DummyCustomType Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return new DummyCustomType(Utils.SliceBuffer(buffer, offset, length));
        }

        public override byte[] Serialize(ushort protocolVersion, DummyCustomType value)
        {
            return value.Buffer;
        }
    }

    public class DummyCustomType
    {
        public byte[] Buffer { get; set; }

        public DummyCustomType(byte[] buffer)
        {
            Buffer = buffer;
        }
    }
}
