//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.Serialization.Primitive
{
    internal class LocalTimeSerializer : TypeSerializer<LocalTime>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Time; }
        }

        public override LocalTime Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return new LocalTime(BeConverter.ToInt64(buffer, offset));
        }

        public override byte[] Serialize(ushort protocolVersion, LocalTime value)
        {
            return BeConverter.GetBytes(value.TotalNanoseconds);
        }
    }
}
