//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Serialization.Primitive
{
    internal class IntSerializer : TypeSerializer<int>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Int; }
        }

        public override int Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return BeConverter.ToInt32(buffer, offset);
        }

        public override byte[] Serialize(ushort protocolVersion, int value)
        {
            return BeConverter.GetBytes(value);
        }
    }
}
