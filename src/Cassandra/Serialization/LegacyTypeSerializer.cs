//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;

namespace Cassandra.Serialization
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
