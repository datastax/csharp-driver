//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.Serialization.Primitive
{
    internal class DoubleSerializer : TypeSerializer<double>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Double; }
        }

        public override double Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return BeConverter.ToDouble(buffer, offset);
        }

        public override byte[] Serialize(ushort protocolVersion, double value)
        {
            return BeConverter.GetBytes(value);
        }
    }
}
