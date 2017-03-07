//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Serialization;

namespace Dse.Test.Unit.Extensions.Serializers
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
