//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Serialization.Primitive
{
    internal class ShortSerializer : TypeSerializer<short>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.SmallInt; }
        }

        public override short Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return BeConverter.ToInt16(buffer, offset);
        }

        public override byte[] Serialize(ushort protocolVersion, short value)
        {
            return BeConverter.GetBytes(value);
        }
    }
}
