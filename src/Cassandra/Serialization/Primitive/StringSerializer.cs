//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Text;

namespace Cassandra.Serialization.Primitive
{
    internal class StringSerializer : TypeSerializer<string>
    {
        private readonly Encoding _encoding;

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Text; }
        }

        public StringSerializer(Encoding encoding)
        {
            _encoding = encoding;
        }

        public override string Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return _encoding.GetString(buffer, offset, length);
        }

        public override byte[] Serialize(ushort protocolVersion, string value)
        {
            return _encoding.GetBytes(value);
        }
    }
}
