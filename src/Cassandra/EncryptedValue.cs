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

namespace Cassandra
{
    /// <summary>
    /// <para>
    /// This struct can be used to "tell" the driver that a specific value should be encrypted before sending it to the server.
    /// You can use this type to effectively use column encryption policies with Simple Statements since the driver can't automatically detect
    /// that a parameter needs to be encrypted when using Simple Statements.
    /// </para>
    /// <para>
    /// To use this type just call the constructor, e.g.:
    /// <code>new SimpleStatement(__CQL_STATEMENT__, new EncryptedValue(__PARAMETER_VALUE__, __ENCRYPTION_KEY__));</code>
    /// </para>
    /// </summary>
    public struct EncryptedValue : IEquatable<EncryptedValue>
    {
        public object Value { get; }

        public object Key { get; }

        public EncryptedValue(object value, object encryptionKey)
        {
            Value = value;
            Key = encryptionKey;
        }

        public bool Equals(EncryptedValue other)
        {
            return Equals(Value, other.Value) && Equals(Key, other.Key);
        }

        public override bool Equals(object obj)
        {
            return obj is EncryptedValue other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Value != null ? Value.GetHashCode() : 0) * 397) ^ (Key != null ? Key.GetHashCode() : 0);
            }
        }

        public static bool operator ==(EncryptedValue left, EncryptedValue right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(EncryptedValue left, EncryptedValue right)
        {
            return !left.Equals(right);
        }
    }
}
