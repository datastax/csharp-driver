//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

namespace Cassandra.Serialization
{
    /// <inheritdoc />
    internal class Serializer : ISerializer
    {
        private readonly IGenericSerializer _serializer;
        
        public Serializer(ProtocolVersion version, IGenericSerializer serializer)
        {
            ProtocolVersion = version;
            _serializer = serializer;
        }

        public ProtocolVersion ProtocolVersion { get; }

        public object Deserialize(byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return _serializer.Deserialize(ProtocolVersion, buffer, offset, length, typeCode, typeInfo);
        }

        public byte[] Serialize(object value)
        {
            return _serializer.Serialize(ProtocolVersion, value);
        }

        public ISerializer CloneWithProtocolVersion(ProtocolVersion version)
        {
            return new Serializer(version, _serializer);
        }

        public object Deserialize(ProtocolVersion version, byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return _serializer.Deserialize(version, buffer, offset, length, typeCode, typeInfo);
        }

        public byte[] Serialize(ProtocolVersion version, object value)
        {
            return _serializer.Serialize(version, value);
        }

        public Type GetClrType(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return _serializer.GetClrType(typeCode, typeInfo);
        }

        public Type GetClrTypeForGraph(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return _serializer.GetClrTypeForGraph(typeCode, typeInfo);
        }

        public Type GetClrTypeForCustom(IColumnInfo typeInfo)
        {
            return _serializer.GetClrTypeForCustom(typeInfo);
        }

        public ColumnTypeCode GetCqlType(Type type, out IColumnInfo typeInfo)
        {
            return _serializer.GetCqlType(type, out typeInfo);
        }

        public bool IsAssignableFrom(CqlColumn column, object value)
        {
            return _serializer.IsAssignableFrom(column, value);
        }

        public UdtMap GetUdtMapByName(string name)
        {
            return _serializer.GetUdtMapByName(name);
        }

        public UdtMap GetUdtMapByType(Type type)
        {
            return _serializer.GetUdtMapByType(type);
        }
    }
}