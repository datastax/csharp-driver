//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Serialization
{
    /// <summary>
    /// Legacy <see cref="ITypeSerializer"/> to support <see cref="ITypeAdapter"/>.
    /// </summary>
    internal class LegacyTypeSerializer : ITypeSerializer
    {
        private readonly ColumnTypeCode _typeCode;
        private readonly ITypeAdapter _adapter;
        private readonly bool _reverse;

        public Type Type
        {
            get { return _adapter.GetDataType(); }
        }

        public IColumnInfo TypeInfo
        {
            get { return null; }
        }

        public ColumnTypeCode CqlType 
        {
            get { return _typeCode; }
        }

        internal LegacyTypeSerializer(ColumnTypeCode typeCode, ITypeAdapter adapter, bool reverse)
        {
            _typeCode = typeCode;
            _adapter = adapter;
            _reverse = reverse;
        }

        public object Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            buffer = Utils.SliceBuffer(buffer, offset, length);
            if (_reverse)
            {
                Array.Reverse(buffer);   
            }
            return _adapter.ConvertFrom(buffer);
        }

        public byte[] Serialize(ushort protocolVersion, object obj)
        {
            var buffer = _adapter.ConvertTo(obj);
            if (_reverse)
            {
                Array.Reverse(buffer);
            }
            return buffer;
        }
    }
}
