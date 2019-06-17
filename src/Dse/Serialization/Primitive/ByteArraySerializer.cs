//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Serialization.Primitive
{
    internal class ByteArraySerializer : TypeSerializer<byte[]>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Blob; }
        }

        public override byte[] Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return Utils.FromOffset(buffer, offset, length);
        }

        public override byte[] Serialize(ushort protocolVersion, byte[] value)
        {
            return value;
        }
    }
}
