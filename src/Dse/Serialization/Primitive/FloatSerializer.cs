//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.Serialization.Primitive
{
    internal class FloatSerializer : TypeSerializer<float>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Float; }
        }

        public override float Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return BeConverter.ToSingle(buffer, offset);
        }

        public override byte[] Serialize(ushort protocolVersion, float value)
        {
            return BeConverter.GetBytes(value);
        }
    }
}
