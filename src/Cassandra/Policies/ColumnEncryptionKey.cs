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
    public struct ColumnEncryptionMetadata : IEquatable<ColumnEncryptionMetadata>
    {
        public ColumnTypeCode TypeCode { get; }

        public IColumnInfo TypeInfo { get; }

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
