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
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;

namespace Cassandra
{
    public abstract class BaseColumnEncryptionPolicy<TKey> : IColumnEncryptionPolicy
    {
        private readonly ConcurrentDictionary<ColDesc, ColData> _colData = new ConcurrentDictionary<ColDesc, ColData>();
        
        public abstract byte[] Encrypt(string ks, string table, string col, byte[] objBytes);

        public abstract byte[] Decrypt(string ks, string table, string col, byte[] encryptedBytes);

        /// <summary>
        /// Provide cryptography materials to be used when encrypted and/or decrypting data
        /// for the specified column.
        /// </summary>
        public void AddColumn(string ks, string table, string col, TKey key, ColumnTypeCode typeCode, IColumnInfo columnTypeInfo)
        {
            ValidateKey(key);

            var colDesc = new ColDesc
            {
                Keyspace = ks,
                Table = table,
                Column = col,
            };
            var colData = new ColData
            {
                Key = key,
                TypeCode = typeCode,
                TypeInfo = columnTypeInfo,
            };

            _colData[colDesc] = colData;
        }

        public Tuple<ColumnTypeCode, IColumnInfo> GetColumn(string ks, string table, string col)
        {
            var found = GetColData(ks, table, col, out var colData);
            return found ? new Tuple<ColumnTypeCode, IColumnInfo>(colData.TypeCode, colData.TypeInfo) : null;
        }

        public void EncodeAndEncrypt(string ks, string table, string col, object obj)
        {
            throw new NotImplementedException();
        }

        protected abstract void ValidateKey(TKey key);

        protected bool GetColData(string ks, string table, string col, out ColData colData)
        {
            var colDesc = new ColDesc
            {
                Column = col,
                Keyspace = ks,
                Table = table,
            };
            return _colData.TryGetValue(colDesc, out colData);
        }

        protected struct ColDesc
        {
            private bool Equals(ColDesc other)
            {
                return Keyspace == other.Keyspace && Table == other.Table && Column == other.Column;
            }

            public override bool Equals(object obj)
            {
                return obj is ColDesc other && Equals(other);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = (Keyspace != null ? Keyspace.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (Table != null ? Table.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (Column != null ? Column.GetHashCode() : 0);
                    return hashCode;
                }
            }

            public string Keyspace { get; set; }

            public string Table { get; set; }

            public string Column { get; set; }
        }

        protected struct ColData
        {
            public TKey Key { get; set; }

            public ColumnTypeCode TypeCode { get; set; }

            public IColumnInfo TypeInfo { get; set; }
        }
    }
}
