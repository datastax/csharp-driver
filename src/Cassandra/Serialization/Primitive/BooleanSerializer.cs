//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.Serialization.Primitive
{
    internal class BooleanSerializer : TypeSerializer<bool>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Boolean; }
        }

        public override bool Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return buffer[offset] == 1;
        }

        public override byte[] Serialize(ushort protocolVersion, bool value)
        {
            return new [] { (byte)(value ? 1 : 0) };
        }
    }
}
