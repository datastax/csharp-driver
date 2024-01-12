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
        private readonly IColumnEncryptionPolicy _columnEncryptionPolicy;

        public Serializer(ProtocolVersion version, IGenericSerializer serializer, IColumnEncryptionPolicy columnEncryptionPolicy)
        {
            ProtocolVersion = version;
            _serializer = serializer;
            _columnEncryptionPolicy = columnEncryptionPolicy;
        }

        public ProtocolVersion ProtocolVersion { get; }

        public object Deserialize(byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return _serializer.Deserialize(ProtocolVersion, buffer, offset, length, typeCode, typeInfo);
        }

        public object DeserializeAndDecrypt(string ks, string table, string column, byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (_columnEncryptionPolicy.ContainsColumn(ks, table, column))
            {
                var colData = _columnEncryptionPolicy.GetColumn(ks, table, column);
                var encryptedData = _serializer.Deserialize(ProtocolVersion, buffer, offset, length, typeCode, typeInfo);
                if (encryptedData == null)
                {
                    throw new DriverInternalError("deserialization of encrypted data returned null");
                }

                var encryptedDataBuf = (byte[])encryptedData;
                var decryptedDataBuf = _columnEncryptionPolicy.Decrypt(ks, table, column, encryptedDataBuf);
                return _serializer.Deserialize(ProtocolVersion, decryptedDataBuf, 0, decryptedDataBuf.Length, colData.Item1, colData.Item2);
            }
            return _serializer.Deserialize(ProtocolVersion, buffer, offset, length, typeCode, typeInfo);
        }

        public byte[] Serialize(object value)
        {
            return _serializer.Serialize(ProtocolVersion, value);
        }
        public byte[] SerializeAndEncrypt(string ks, string table, string column, object value)
        {
            var serialized = _serializer.Serialize(ProtocolVersion, value);
            if (_columnEncryptionPolicy.ContainsColumn(ks, table, column))
            {
                serialized = _columnEncryptionPolicy.Encrypt(ks, table, column, serialized);
                serialized = _serializer.Serialize(ProtocolVersion, serialized);
            }

            return serialized;
        }

        public ISerializer CloneWithProtocolVersion(ProtocolVersion version)
        {
            return new Serializer(version, _serializer, _columnEncryptionPolicy);
        }

        public bool IsEncryptionEnabled => _columnEncryptionPolicy != null;

        public Tuple<bool, ColumnTypeCode> IsAssignableFromEncrypted(string ks, string table, string column, ColumnTypeCode columnTypeCode, object value)
        {
            var colData = _columnEncryptionPolicy.GetColumn(ks, table, column);
            return new Tuple<bool, ColumnTypeCode>(IsAssignableFrom(colData?.Item1 ?? columnTypeCode, value), colData?.Item1 ?? columnTypeCode);
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

        public bool IsAssignableFrom(ColumnTypeCode columnTypeCode, object value)
        {
            return _serializer.IsAssignableFrom(columnTypeCode, value);
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