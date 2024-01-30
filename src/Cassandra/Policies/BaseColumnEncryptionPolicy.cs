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

namespace Cassandra
{
    /// <summary>
    /// This abstract class provides functionality to manage the column encryption metadata of encrypted columns. You can implement a custom ColumnEncryptionPolicy
    /// by inheriting this class and overriding <see cref="EncryptWithKey"/> and <see cref="DecryptWithKey"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the "key" object that is used by the implementations of this class. See an example of this in <see cref="AesColumnEncryptionPolicy"/>.
    /// This is only meant to provide some compile time type safety since the base interface works with the basic "object" type.</typeparam>
    public abstract class BaseColumnEncryptionPolicy<TKey>: IColumnEncryptionPolicy
    {
        private readonly ConcurrentDictionary<ColMetadataKey, ColumnEncryptionMetadata> _colData = new ConcurrentDictionary<ColMetadataKey, ColumnEncryptionMetadata>();


        /// <inheritdoc />
        public byte[] Encrypt(object key, byte[] objBytes)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key is TKey typedKey)
            {
                return EncryptWithKey(typedKey, objBytes);
            }

            throw new ArgumentException($"invalid key type, expected {typeof(TKey).AssemblyQualifiedName} but got {key.GetType().AssemblyQualifiedName}");
        }

        /// <inheritdoc />
        public byte[] Decrypt(object key, byte[] encryptedBytes)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key is TKey typedKey)
            {
                return DecryptWithKey(typedKey, encryptedBytes);
            }

            throw new ArgumentException($"invalid key type, expected {typeof(TKey).AssemblyQualifiedName} but got {key.GetType().AssemblyQualifiedName}");
        }

        /// <summary>
        /// Encrypts the provided byte array (serialized value) with the provided key (which was previously added with the <see cref="AddColumn(string,string,string,TKey,Cassandra.ColumnTypeCode)"/> method).
        /// </summary>
        /// <remarks>
        /// Implement your column encryption policy encryption logic by overriding this method.
        /// </remarks>
        /// <param name="key">Key that was previously provided with <see cref="AddColumn(string,string,string,TKey,Cassandra.ColumnTypeCode)"/>.</param>
        /// <param name="objBytes">Serialized value as a byte array.</param>
        /// <returns>The encrypted bytes.</returns>
        public abstract byte[] EncryptWithKey(TKey key, byte[] objBytes);

        /// <summary>
        /// Decrypts the provided encrypted byte array with the provided key (which was previously added with the <see cref="AddColumn(string,string,string,TKey,Cassandra.ColumnTypeCode)"/> method).
        /// </summary>
        /// <remarks>
        /// Implement your column encryption policy decryption logic by overriding this method.
        /// </remarks>
        /// <param name="key">Key that was previously provided with <see cref="AddColumn(string,string,string,TKey,Cassandra.ColumnTypeCode)"/>.</param>
        /// <param name="encryptedBytes">Encrypted bytes read from the server.</param>
        /// <returns>The decrypted bytes (i.e. serialized value) which will then be deserialized by the driver afterwards.</returns>
        public abstract byte[] DecryptWithKey(TKey key, byte[] encryptedBytes);

        /// <summary>
        /// Provide cryptography materials to be used when encrypted and/or decrypting data
        /// for the specified column.
        /// </summary>
        /// <remarks>This overload has an extra parameter (<paramref name="columnTypeInfo"/>) which is used if the <paramref name="typeCode"/> refers to a type that requires extra type information.
        /// E.g. collection types require information about the type of objects that the collection contains. This overload should only be used if the column is of type 'map', 'list', 'set', 'udt', 'tuple' or 'custom'. </remarks>
        public virtual void AddColumn(string ks, string table, string col, TKey key, ColumnTypeCode typeCode, IColumnInfo columnTypeInfo)
        {
            var colDesc = new ColMetadataKey
            {
                Keyspace = ks,
                Table = table,
                Column = col,
            };
            var colData = new ColumnEncryptionMetadata(typeCode, columnTypeInfo, key);

            _colData[colDesc] = colData;
        }

        /// <summary>
        /// Provide cryptography materials to be used when encrypting and/or decrypting data
        /// for the specified column.
        /// </summary>
        /// <remarks>If the <paramref name="typeCode"/> is 'map','list','set','udt','tuple' or 'custom' then you have to use the other overload
        /// (<see cref="AddColumn(string,string,string,TKey,Cassandra.ColumnTypeCode,Cassandra.IColumnInfo)"/>) so you can provide the <see cref="IColumnInfo"/>.</remarks>
        public virtual void AddColumn(string ks, string table, string col, TKey key, ColumnTypeCode typeCode)
        {
            var colDesc = new ColMetadataKey
            {
                Keyspace = ks,
                Table = table,
                Column = col,
            };
            var colData = new ColumnEncryptionMetadata(typeCode, key);

            _colData[colDesc] = colData;
        }

        /// <inheritdoc />
        public virtual ColumnEncryptionMetadata? GetColumnEncryptionMetadata(string ks, string table, string col)
        {
            var colDesc = new ColMetadataKey
            {
                Column = col,
                Keyspace = ks,
                Table = table,
            };
            var found = _colData.TryGetValue(colDesc, out var metadata);
            if (!found)
            {
                return null;
            }
            return metadata;
        }

        /// <summary>
        /// Type used by the internal map that manages the metadata of the encrypted columns. It is used to check whether a particular column is encrypted or not.
        /// </summary>
        protected struct ColMetadataKey : IEquatable<ColMetadataKey>
        {
            public static bool operator ==(ColMetadataKey left, ColMetadataKey right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(ColMetadataKey left, ColMetadataKey right)
            {
                return !left.Equals(right);
            }

            public bool Equals(ColMetadataKey other)
            {
                return Keyspace == other.Keyspace && Table == other.Table && Column == other.Column;
            }

            public override bool Equals(object obj)
            {
                return obj is ColMetadataKey other && Equals(other);
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

            /// <summary>
            /// Keyspace of the encrypted column's table.
            /// </summary>
            public string Keyspace { get; set; }

            /// <summary>
            /// The encrypted column's table.
            /// </summary>
            public string Table { get; set; }

            /// <summary>
            /// The encrypted column name.
            /// </summary>
            public string Column { get; set; }
        }
    }
}
