//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Serialization.Primitive
{
    internal class GuidSerializer : TypeSerializer<Guid>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Uuid; }
        }

        public override Guid Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return new Guid(GuidShuffle(buffer, offset));
        }

        public override byte[] Serialize(ushort protocolVersion, Guid value)
        {
            return GuidShuffle(value.ToByteArray());
        }
    }
}
