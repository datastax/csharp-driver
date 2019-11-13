//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.Serialization.Primitive
{
    /// <summary>
    /// A serializer for CQL type bigint, CLR type Int64.
    /// </summary>
    internal class LongSerializer : TypeSerializer<long>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Bigint; }
        }

        public override long Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return BeConverter.ToInt64(buffer, offset);
        }

        public override byte[] Serialize(ushort protocolVersion, long value)
        {
            return BeConverter.GetBytes(value);
        }
    }
}
