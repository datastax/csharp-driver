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
