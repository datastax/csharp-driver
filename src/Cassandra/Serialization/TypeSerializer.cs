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
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using System.Text;
using Cassandra.Serialization.Primitive;

namespace Cassandra.Serialization
{
    /// <summary>
    /// A serializer that can encode and decode to and from a given CQL type and a given CLR Type.
    /// </summary>
    public abstract class TypeSerializer
    {
        public static readonly TypeSerializer<BigInteger> PrimitiveBigIntegerSerializer = new BigIntegerSerializer();
        public static readonly TypeSerializer<bool> PrimitiveBooleanSerializer = new BooleanSerializer();
        public static readonly TypeSerializer<byte[]> PrimitiveByteArraySerializer = new ByteArraySerializer();
        public static readonly TypeSerializer<DateTimeOffset> PrimitiveDateTimeOffsetSerializer = new DateTimeOffsetSerializer();
        public static readonly TypeSerializer<DateTime> PrimitiveDateTimeSerializer = new DateTimeSerializer();
        public static readonly TypeSerializer<decimal> PrimitiveDecimalSerializer = new DecimalSerializer();
        public static readonly TypeSerializer<double> PrimitiveDoubleSerializer = new DoubleSerializer();
        public static readonly TypeSerializer<Duration> PrimitiveDurationSerializer = new DurationSerializer(true);
        public static readonly TypeSerializer<float> PrimitiveFloatSerializer = new FloatSerializer();
        public static readonly TypeSerializer<Guid> PrimitiveGuidSerializer = new GuidSerializer();
        public static readonly TypeSerializer<int> PrimitiveIntSerializer = new IntSerializer();
        public static readonly TypeSerializer<IPAddress> PrimitiveIpAddressSerializer = new IpAddressSerializer();
        public static readonly TypeSerializer<LocalDate> PrimitiveLocalDateSerializer = new LocalDateSerializer();
        public static readonly TypeSerializer<LocalTime> PrimitiveLocalTimeSerializer = new LocalTimeSerializer();
        public static readonly TypeSerializer<long> PrimitiveLongSerializer = new LongSerializer();
        public static readonly TypeSerializer<sbyte> PrimitiveSbyteSerializer = new SbyteSerializer();
        public static readonly TypeSerializer<short> PrimitiveShortSerializer = new ShortSerializer();
        public static readonly TypeSerializer<string> PrimitiveStringSerializer = new StringSerializer(Encoding.UTF8);
        public static readonly TypeSerializer<string> PrimitiveAsciiStringSerializer = new StringSerializer(Encoding.ASCII);
        public static readonly TypeSerializer<TimeUuid> PrimitiveTimeUuidSerializer = new TimeUuidSerializer();

        internal static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        internal static byte[] GuidShuffle(byte[] b, int offset = 0)
        {
            return new[]
            {
                b[offset + 3], b[offset + 2], b[offset + 1], b[offset + 0], 
                b[offset + 5], b[offset + 4], 
                b[offset + 7], b[offset + 6], 
                b[offset + 8], b[offset + 9], b[offset + 10], b[offset + 11], b[offset + 12], b[offset + 13], b[offset + 14], b[offset + 15]
            };
        }

        /// <summary>
        /// Decodes length for collection types depending on the protocol version
        /// </summary>
        internal static int DecodeCollectionLength(ProtocolVersion protocolVersion, byte[] buffer, ref int index)
        {
            int result;
            if (!protocolVersion.Uses4BytesCollectionLength())
            {
                //length is a short
                result = BeConverter.ToInt16(buffer, index);
                index += 2;
            }
            else
            {
                //length is expressed in int
                result = BeConverter.ToInt32(buffer, index);
                index += 4;
            }
            return result;
        }

        /// <summary>
        /// Uses 2 or 4 bytes to represent the length in bytes
        /// </summary>
        internal static byte[] EncodeCollectionLength(ushort protocolVersion, int value)
        {
            if (protocolVersion < 3)
            {
                return BeConverter.GetBytes((short)value);
            }
            return BeConverter.GetBytes(value);
        }

        internal static byte[] EncodeBufferList(ICollection<byte[]> bufferList, int bufferLength)
        {
            //Add the necessary bytes length per each [bytes]
            bufferLength += bufferList.Count * 4;
            var result = new byte[bufferLength];
            var index = 0;
            foreach (var buf in bufferList)
            {
                var bufferItemLength = BeConverter.GetBytes(buf != null ? buf.Length : -1);
                Buffer.BlockCopy(bufferItemLength, 0, result, index, bufferItemLength.Length);
                index += bufferItemLength.Length;
                if (buf == null)
                {
                    continue;
                }
                Buffer.BlockCopy(buf, 0, result, index, buf.Length);
                index += buf.Length;
            }
            return result;
        }

        /// <summary>
        /// Returns the time interval since unix epoch.
        /// </summary>
        public static TimeSpan SinceUnixEpoch(DateTimeOffset value)
        {
            return value - UnixStart;
        }

        /// <summary>
        /// Checks the expected type of an object
        /// </summary>
        internal static void CheckArgument<T>(object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException();
            }
            if (!(value is T))
            {
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] { typeof(T).FullName });
            }
        }
    }

    /// <summary>
    /// An abstract class that represents a serializer that can encode and decode to and from a given CQL type and a
    /// given CLR Type.
    /// </summary>
    /// <typeparam name="T">CLR type for this serializer</typeparam>
    public abstract class TypeSerializer<T> : TypeSerializer, ITypeSerializer
    {
        private IGenericSerializer _serializer;
        /// <summary>
        /// Gets the CLR type for this serializer.
        /// </summary>
        public Type Type
        {
            get { return typeof(T); }
        }

        /// <summary>
        /// Gets the type information for which this serializer is valid.
        /// <para>
        /// Intended for non-primitive types such as custom types and UDTs.
        /// For primitive types, it should return <c>null</c>.
        /// </para>
        /// </summary>
        public virtual IColumnInfo TypeInfo
        {
            get { return null;}
        }

        /// <summary>
        /// Returns the Cassandra data type for the serializer.
        /// </summary>
        public abstract ColumnTypeCode CqlType { get; }

        object ITypeSerializer.Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return Deserialize(protocolVersion, buffer, offset, length, typeInfo);
        }

        byte[] ITypeSerializer.Serialize(ushort protocolVersion, object obj)
        {
            return Serialize(protocolVersion, (T)obj);
        }

        /// <summary>
        /// When overridden from a derived class, it reads the byte buffer and returns the CLR representation of the
        /// data type.
        /// </summary>
        /// <param name="protocolVersion">The Cassandra native protocol version.</param>
        /// <param name="buffer">The byte array.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin storing data from the current stream.</param>
        /// <param name="length">The maximum amount of bytes to read from buffer.</param>
        /// <param name="typeInfo">Additional type information designed for non-primitive types.</param>
        public abstract T Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo);

        /// <summary>
        /// When overridden from a derived class, it encodes the CLR object into the byte representation
        /// according to the Cassandra native protocol.
        /// </summary>
        /// <param name="protocolVersion">The Cassandra native protocol version.</param>
        /// <param name="value">The object to encode.</param>
        public abstract byte[] Serialize(ushort protocolVersion, T value);

        internal object DeserializeChild(ushort protocolVersion, byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.Deserialize((ProtocolVersion)protocolVersion, buffer, offset, length, typeCode, typeInfo);
        }

        internal Type GetClrType(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.GetClrType(typeCode, typeInfo);
        }
        
        internal Type GetClrTypeForGraph(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.GetClrTypeForGraph(typeCode, typeInfo);
        }

        internal byte[] SerializeChild(ushort protocolVersion, object obj)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.Serialize((ProtocolVersion)protocolVersion, obj);
        }

        internal void SetChildSerializer(IGenericSerializer serializer)
        {
            _serializer = serializer;
        }
    }
}
