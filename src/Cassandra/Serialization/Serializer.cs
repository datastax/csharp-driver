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
            var columnEncryptionMetadata = _columnEncryptionPolicy.GetColumnEncryptionMetadata(ks, table, column);
            if (columnEncryptionMetadata != null)
            {
                var encryptedData = _serializer.Deserialize(ProtocolVersion, buffer, offset, length, typeCode, typeInfo);
                if (encryptedData == null)
                {
                    throw new DriverInternalError("deserialization of encrypted data returned null");
                }

                if (!(encryptedData is byte[] encryptedDataBuf))
                {
                    throw new ColumnEncryptionInvalidTypeError(column, typeCode, encryptedData);
                }

                var decryptedDataBuf = _columnEncryptionPolicy.Decrypt(columnEncryptionMetadata.Value.Key, encryptedDataBuf);
                if (decryptedDataBuf == null)
                {
                    return null;
                }
                return _serializer.Deserialize(ProtocolVersion, decryptedDataBuf, 0, decryptedDataBuf.Length, columnEncryptionMetadata.Value.TypeCode, columnEncryptionMetadata.Value.TypeInfo);
            }
            return _serializer.Deserialize(ProtocolVersion, buffer, offset, length, typeCode, typeInfo);
        }

        public byte[] Serialize(object value)
        {
            return _serializer.Serialize(ProtocolVersion, value);
        }

        public byte[] SerializeAndEncrypt(string defaultKs, RowSetMetadata metadata, int colIdx, object[] values, int valueIdx)
        {
            if (values == null)
            {
                throw new InvalidOperationException("could not serialize value because values array is null");
            }

            if (valueIdx >= values.Length)
            {
                throw new InvalidOperationException($"could not serialize value because value index {valueIdx} is out of bounds {values.Length}");
            }

            var value = values[valueIdx];

            if (!IsEncryptionEnabled)
            {
                return Serialize(value);
            }

            byte[] serialized;
            if (value is EncryptedValue markedValue)
            {
                serialized = _serializer.Serialize(ProtocolVersion, markedValue.Value);
                serialized = _columnEncryptionPolicy.Encrypt(markedValue.Key, serialized);
                return _serializer.Serialize(ProtocolVersion, serialized);
            }

            serialized = _serializer.Serialize(ProtocolVersion, value);

            if (metadata == null)
            {
                // probably simple statement
                return serialized;
            }

            if (metadata.Columns == null)
            {
                throw new InvalidOperationException("column metadata is null");
            }

            if (colIdx >= metadata.Columns.Length)
            {
                throw new InvalidOperationException($"column index is {colIdx}, columns length is {metadata.Columns.Length}");
            }

            var colMetadata = metadata.Columns[colIdx];
            var encryptionMetadata = _columnEncryptionPolicy.GetColumnEncryptionMetadata(
                colMetadata.Keyspace ?? metadata.Keyspace ?? defaultKs, colMetadata.Table, colMetadata.Name);
            if (encryptionMetadata == null)
            {
                return serialized;
            }

            serialized = _columnEncryptionPolicy.Encrypt(encryptionMetadata.Value.Key, serialized);
            return _serializer.Serialize(ProtocolVersion, serialized);

        }

        public ISerializer CloneWithProtocolVersion(ProtocolVersion version)
        {
            return new Serializer(version, _serializer, _columnEncryptionPolicy);
        }

        public bool IsEncryptionEnabled => _columnEncryptionPolicy != null;

        public Tuple<bool, ColumnTypeCode> IsAssignableFromEncrypted(string ks, string table, string column, ColumnTypeCode columnTypeCode, object value)
        {
            var colData = _columnEncryptionPolicy.GetColumnEncryptionMetadata(ks, table, column);
            return new Tuple<bool, ColumnTypeCode>(IsAssignableFrom(colData?.TypeCode ?? columnTypeCode, value), colData?.TypeCode ?? columnTypeCode);
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