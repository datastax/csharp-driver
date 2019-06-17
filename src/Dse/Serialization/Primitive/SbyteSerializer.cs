//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Serialization.Primitive
{
    internal class SbyteSerializer : TypeSerializer<sbyte>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.TinyInt; }
        }

        public override sbyte Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return unchecked((sbyte)buffer[offset]);
        }

        public override byte[] Serialize(ushort protocolVersion, sbyte value)
        {
            return new[] { unchecked((byte)value) };
        }
    }
}
