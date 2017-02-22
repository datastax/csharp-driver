//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Numerics;

namespace Dse.Serialization.Primitive
{
    /// <summary>
    /// A serializer for CQL type varint, CLR type BigInteger.
    /// </summary>
    internal class BigIntegerSerializer : TypeSerializer<BigInteger>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Varint; }
        }

        public override BigInteger Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            buffer = Utils.SliceBuffer(buffer, offset, length);
            //Cassandra uses big endian encoding
            Array.Reverse(buffer);
            return new BigInteger(buffer);
        }

        public override byte[] Serialize(ushort protocolVersion, BigInteger value)
        {
            var buffer = value.ToByteArray();
            //Cassandra expects big endian encoding
            Array.Reverse(buffer);
            return buffer;
        }
    }
}
