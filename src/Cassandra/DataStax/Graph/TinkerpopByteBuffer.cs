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

namespace Cassandra.DataStax.Graph
{
    
    /// <summary>
    /// Represents a value of type "ByteBuffer" in Tinkerpop. It's basically a wrapper around a <see cref="byte"/> array.
    /// </summary>
    public struct TinkerpopByteBuffer : IEquatable<TinkerpopByteBuffer>, IEquatable<byte[]>
    {
        public TinkerpopByteBuffer(byte[] value)
        {
            Value = value;
        }

        public byte[] AsByteArray() => (byte[]) this;

        public byte[] Value { get; }
        
        public static implicit operator TinkerpopByteBuffer(byte[] obj) => new TinkerpopByteBuffer(obj);

        public static implicit operator byte[](TinkerpopByteBuffer obj) => obj.Value;

        public static bool operator ==(TinkerpopByteBuffer i1, TinkerpopByteBuffer i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(TinkerpopByteBuffer i1, TinkerpopByteBuffer i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(TinkerpopByteBuffer i1, byte[] i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(TinkerpopByteBuffer i1, byte[] i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(byte[] i1, TinkerpopByteBuffer i2)
        {
            return i2.Equals(i1);
        }

        public static bool operator !=(byte[] i1, TinkerpopByteBuffer i2)
        {
            return !(i1 == i2);
        }

        public bool Equals(TinkerpopByteBuffer other)
        {
            return Value.Equals(other.Value);
        }

        public bool Equals(byte[] other)
        {
            return AsByteArray().Equals(other);
        }

        public override int GetHashCode()
        {
            return -1937119414 + Value.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            switch (obj)
            {
                case null:
                    return false;
                case byte[] arr:
                    return Equals(arr);
                case TinkerpopByteBuffer i:
                    return Equals(i);
                default:
                    return false;
            }
        }

        public override string ToString()
        {
            return Value.ToString();
        }
    }
}