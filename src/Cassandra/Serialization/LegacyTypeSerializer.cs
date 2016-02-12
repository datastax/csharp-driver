using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Serialization
{
    /// <summary>
    /// Legacy <see cref="ITypeSerializer"/> to support <see cref="ITypeAdapter"/>.
    /// </summary>
    internal class LegacyTypeSerializer : ITypeSerializer
    {
        private readonly ColumnTypeCode _typeCode;
        private readonly ITypeAdapter _adapter;

        public Type Type
        {
            get { return _adapter.GetDataType(); }
        }

        public ColumnTypeCode CqlType 
        {
            get { return _typeCode; }
        }

        internal LegacyTypeSerializer(ColumnTypeCode typeCode, ITypeAdapter adapter)
        {
            _typeCode = typeCode;
            _adapter = adapter;
        }

        public object Deserialize(ushort protocolVersion, byte[] buffer, IColumnInfo typeInfo)
        {
            return _adapter.ConvertFrom(buffer);
        }

        public byte[] Serialize(ushort protocolVersion, object obj)
        {
            return _adapter.ConvertTo(obj);
        }
    }
}
