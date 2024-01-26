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
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// Utility type used in <see cref="IColumnEncryptionPolicy"/> to provide the driver with the encryption key and
    /// "real" cql type of the column (the server side type should always be 'blob' for encrypted columns but the type at the application level can be any cql type)
    /// </summary>
    public struct ColumnEncryptionMetadata : IEquatable<ColumnEncryptionMetadata>
    {
        /// <summary>
        /// CQL Type code of the encrypted column at the application level.
        /// The driver will decrypt the encrypted column data and then deserialize it to the type provided here.
        /// You must ensure that you are providing a .NET value that matches this CQL type code when using encrypted parameter values.
        /// </summary>
        public ColumnTypeCode TypeCode { get; }

        /// <summary>
        /// This must be provided in addition to <see cref="TypeCode"/> if the type code refers to 'list','set','map','udt','tuple' or 'custom'.
        /// Each of these cql types has a ColumnInfo class associated with it (e.g. <see cref="ListColumnInfo"/> for 'list').
        /// </summary>
        public IColumnInfo TypeInfo { get; }

        /// <summary>
        /// <para>
        /// Key object that will provided to the <see cref="IColumnEncryptionPolicy.Encrypt"/> and <see cref="IColumnEncryptionPolicy.Decrypt"/> methods.
        /// The type that is used for this key has to be a type that is "understood" by the particular implementation of <see cref="IColumnEncryptionPolicy"/> that is used.
        /// </para>
        /// <para>
        /// E.g. For <see cref="AesColumnEncryptionPolicy"/> this key should be an instance of <see cref="AesColumnEncryptionPolicy.AesKeyAndIV"/> (which is forced by
        /// the <see cref="AesColumnEncryptionPolicy.AddColumn(string,string,string,AesColumnEncryptionPolicy.AesKeyAndIV,Cassandra.ColumnTypeCode)"/> method.
        /// </para>
        /// </summary>
        public object Key { get; }

        public ColumnEncryptionMetadata(ColumnTypeCode typeCode, object key) : this(typeCode, null, key)
        {
        }

        public ColumnEncryptionMetadata(ColumnTypeCode typeCode, IColumnInfo typeInfo, object key)
        {
            GenericSerializer.ValidateColumnInfo(typeCode, typeInfo);
            TypeCode = typeCode;
            TypeInfo = typeInfo;
            Key = key;
        }
        public bool Equals(ColumnEncryptionMetadata other)
        {
            return TypeCode == other.TypeCode && Equals(TypeInfo, other.TypeInfo) && Equals(Key, other.Key);
        }

        public override bool Equals(object obj)
        {
            return obj is ColumnEncryptionMetadata other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (int)TypeCode;
                hashCode = (hashCode * 397) ^ (TypeInfo != null ? TypeInfo.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Key != null ? Key.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(ColumnEncryptionMetadata left, ColumnEncryptionMetadata right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(ColumnEncryptionMetadata left, ColumnEncryptionMetadata right)
        {
            return !left.Equals(right);
        }
    }
}
