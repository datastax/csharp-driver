//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using System.Text;
using System.Linq;

namespace Cassandra
{
    internal delegate object DecodeHandler(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType);
    internal delegate byte[] EncodeHandler(int protocolVersion, IColumnInfo typeInfo, object value);

    /// <summary>
    /// Contains the methods handle serialization and deserialization from Cassandra types to CLR types
    /// </summary>
    internal static class TypeCodec
    {
        private const string ListTypeName = "org.apache.cassandra.db.marshal.ListType";
        private const string SetTypeName = "org.apache.cassandra.db.marshal.SetType";
        private const string MapTypeName = "org.apache.cassandra.db.marshal.MapType";
        private const string UdtTypeName = "org.apache.cassandra.db.marshal.UserType";
        private const string TupleTypeName = "org.apache.cassandra.db.marshal.TupleType";
        private const string FrozenTypeName = "org.apache.cassandra.db.marshal.FrozenType";
        public const string ReversedTypeName = "org.apache.cassandra.db.marshal.ReversedType";
        public const string CompositeTypeName = "org.apache.cassandra.db.marshal.CompositeType";
        /// <summary>
        /// An instance of a buffer that represents the value Unset
        /// </summary>
        internal static readonly byte[] UnsetBuffer = new byte[0];
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);
        private static readonly ConcurrentDictionary<string, UdtMap> UdtMapsByName = new ConcurrentDictionary<string, UdtMap>();
        private static readonly ConcurrentDictionary<Type, UdtMap> UdtMapsByClrType = new ConcurrentDictionary<Type, UdtMap>();

        /// <summary>
        /// Decoders by type code
        /// </summary>
        private static readonly Dictionary<ColumnTypeCode, EncodeHandler> Encoders = new Dictionary<ColumnTypeCode, EncodeHandler>()
        {
            {ColumnTypeCode.Ascii,        EncodeAscii},
            {ColumnTypeCode.Bigint,       EncodeBigint},
            {ColumnTypeCode.Blob,         EncodeBlob},
            {ColumnTypeCode.Boolean,      EncodeBoolean},
            {ColumnTypeCode.Counter,      EncodeCounter},
            {ColumnTypeCode.Custom,       EncodeCustom},
            {ColumnTypeCode.Double,       EncodeDouble},
            {ColumnTypeCode.Float,        EncodeFloat},
            {ColumnTypeCode.Int,          EncodeInt},
            {ColumnTypeCode.Text,         EncodeText},
            {ColumnTypeCode.Timestamp,    EncodeTimestamp},
            {ColumnTypeCode.Uuid,         EncodeUuid},
            {ColumnTypeCode.Varchar,      EncodeVarchar},
            {ColumnTypeCode.Timeuuid,     EncodeTimeuuid},
            {ColumnTypeCode.Inet,         EncodeInet},
            {ColumnTypeCode.Date,         EncodeDate},
            {ColumnTypeCode.Time,         EncodeTime},
            {ColumnTypeCode.SmallInt,     EncodeShort},
            {ColumnTypeCode.TinyInt,      EncodeSByte},
            {ColumnTypeCode.List,         EncodeList},
            {ColumnTypeCode.Map,          EncodeMap},
            {ColumnTypeCode.Set,          EncodeSet},
            {ColumnTypeCode.Decimal,      EncodeDecimal},
            {ColumnTypeCode.Varint,       EncodeVarint},
            {ColumnTypeCode.Udt,          EncodeUdt},
            {ColumnTypeCode.Tuple,        EncodeTuple}
        };

        /// <summary>
        /// Decoders by type code, taking the raw bytes and reconstructing the object model.
        /// </summary>
        private static readonly Dictionary<ColumnTypeCode, DecodeHandler> Decoders = new Dictionary<ColumnTypeCode, DecodeHandler>()
        {
            {ColumnTypeCode.Ascii,        DecodeAscii},
            {ColumnTypeCode.Bigint,       DecodeBigint},
            {ColumnTypeCode.Blob,         DecodeBlob},
            {ColumnTypeCode.Boolean,      DecodeBoolean},
            {ColumnTypeCode.Counter,      DecodeCounter},
            {ColumnTypeCode.Custom,       DecodeCustom},
            {ColumnTypeCode.Double,       DecodeDouble},
            {ColumnTypeCode.Float,        DecodeFloat},
            {ColumnTypeCode.Int,          DecodeInt},
            {ColumnTypeCode.Text,         DecodeText},
            {ColumnTypeCode.Timestamp,    DecodeTimestamp},
            {ColumnTypeCode.Uuid,         DecodeUuid},
            {ColumnTypeCode.Varchar,      DecodeVarchar},
            {ColumnTypeCode.Timeuuid,     DecodeTimeuuid},
            {ColumnTypeCode.Inet,         DecodeInet},
            {ColumnTypeCode.Date,         DecodeDate},
            {ColumnTypeCode.Time,         DecodeTime},
            {ColumnTypeCode.SmallInt,     DecodeShort},
            {ColumnTypeCode.TinyInt,      DecodeSByte},
            {ColumnTypeCode.List,         DecodeList},
            {ColumnTypeCode.Map,          DecodeMap},
            {ColumnTypeCode.Set,          DecodeSet},
            {ColumnTypeCode.Decimal,      DecodeDecimal},
            {ColumnTypeCode.Varint,       DecodeVarint},
            {ColumnTypeCode.Udt,          DecodeUdt},
            {ColumnTypeCode.Tuple,        DecodeTuple}
        };

        /// <summary>
        /// Default CLR type by type code
        /// </summary>
        private static readonly Dictionary<ColumnTypeCode, DefaultTypeFromCqlTypeDelegate> DefaultTypes = new Dictionary<ColumnTypeCode, DefaultTypeFromCqlTypeDelegate>()
        {
            {ColumnTypeCode.Ascii,        _ => typeof (string)},
            {ColumnTypeCode.Bigint,       _ => typeof (long)},
            {ColumnTypeCode.Blob,         _ => typeof (byte[])},
            {ColumnTypeCode.Boolean,      _ => typeof (bool)},
            {ColumnTypeCode.Counter,      _ => typeof (long)},
            {ColumnTypeCode.Custom,       GetDefaultTypeFromCustom},
            {ColumnTypeCode.Double,       _ => typeof (double)},
            {ColumnTypeCode.Float,        _ => typeof (float)},
            {ColumnTypeCode.Int,          _ => typeof (int)},
            {ColumnTypeCode.Text,         _ => typeof (string)},
            {ColumnTypeCode.Timestamp,    _ => typeof (DateTimeOffset)},
            {ColumnTypeCode.Uuid,         _ => typeof (Guid)},
            {ColumnTypeCode.Varchar,      _ => typeof (string)},
            {ColumnTypeCode.Timeuuid,     _ => typeof (Guid)},
            {ColumnTypeCode.Inet,         _ => typeof (IPAddress)},
            {ColumnTypeCode.Date,         _ => typeof (LocalDate)},
            {ColumnTypeCode.Time,         _ => typeof (LocalTime)},
            {ColumnTypeCode.SmallInt,     _ => typeof (short)},
            {ColumnTypeCode.TinyInt,      _ => typeof (sbyte)},
            {ColumnTypeCode.List,         GetDefaultTypeFromList},
            {ColumnTypeCode.Map,          GetDefaultTypeFromMap},
            {ColumnTypeCode.Set,          GetDefaultTypeFromSet},
            {ColumnTypeCode.Decimal,      GetDefaultTypeFromDecimal},
            {ColumnTypeCode.Varint,       GetDefaultTypeFromVarint},
            {ColumnTypeCode.Udt,          GetDefaultTypeFromUdt},
            {ColumnTypeCode.Tuple,        GetDefaultTypeFromTuple}
        };

        /// <summary>
        /// Default single (no collection types) cql type per CLR type
        /// </summary>
        private readonly static Dictionary<Type, ColumnTypeCode> DefaultSingleCqlTypes = new Dictionary<Type, ColumnTypeCode>()
        {
            { typeof(string), ColumnTypeCode.Text },
            { typeof(long), ColumnTypeCode.Bigint },
            { typeof(byte[]), ColumnTypeCode.Blob },
            { typeof(bool), ColumnTypeCode.Boolean },
            { typeof(double), ColumnTypeCode.Double },
            { typeof(float), ColumnTypeCode.Float },
            { typeof(IPAddress), ColumnTypeCode.Inet },
            { typeof(int), ColumnTypeCode.Int },
            { typeof(DateTimeOffset), ColumnTypeCode.Timestamp },
            { typeof(DateTime), ColumnTypeCode.Timestamp },
            { typeof(Guid), ColumnTypeCode.Uuid },
            { typeof(TimeUuid), ColumnTypeCode.Timeuuid },
            { typeof(LocalDate), ColumnTypeCode.Date },
            { typeof(LocalTime), ColumnTypeCode.Time },
            { typeof(short), ColumnTypeCode.SmallInt },
            { typeof(sbyte), ColumnTypeCode.TinyInt },
            { TypeAdapters.DecimalTypeAdapter.GetDataType(), ColumnTypeCode.Decimal },
            { TypeAdapters.VarIntTypeAdapter.GetDataType(), ColumnTypeCode.Varint }
        };

        private static readonly Dictionary<string, ColumnTypeCode> SingleTypeNames = new Dictionary<string, ColumnTypeCode>()
        {
            {"org.apache.cassandra.db.marshal.UTF8Type", ColumnTypeCode.Varchar},
            {"org.apache.cassandra.db.marshal.AsciiType", ColumnTypeCode.Ascii},
            {"org.apache.cassandra.db.marshal.UUIDType", ColumnTypeCode.Uuid},
            {"org.apache.cassandra.db.marshal.TimeUUIDType", ColumnTypeCode.Timeuuid},
            {"org.apache.cassandra.db.marshal.Int32Type", ColumnTypeCode.Int},
            {"org.apache.cassandra.db.marshal.BytesType", ColumnTypeCode.Blob},
            {"org.apache.cassandra.db.marshal.FloatType", ColumnTypeCode.Float},
            {"org.apache.cassandra.db.marshal.DoubleType", ColumnTypeCode.Double},
            {"org.apache.cassandra.db.marshal.BooleanType", ColumnTypeCode.Boolean},
            {"org.apache.cassandra.db.marshal.InetAddressType", ColumnTypeCode.Inet},
            {"org.apache.cassandra.db.marshal.SimpleDateType", ColumnTypeCode.Date},
            {"org.apache.cassandra.db.marshal.TimeType", ColumnTypeCode.Time},
            {"org.apache.cassandra.db.marshal.ShortType", ColumnTypeCode.SmallInt},
            {"org.apache.cassandra.db.marshal.ByteType", ColumnTypeCode.TinyInt},
            {"org.apache.cassandra.db.marshal.DateType", ColumnTypeCode.Timestamp},
            {"org.apache.cassandra.db.marshal.TimestampType", ColumnTypeCode.Timestamp},
            {"org.apache.cassandra.db.marshal.LongType", ColumnTypeCode.Bigint},
            {"org.apache.cassandra.db.marshal.DecimalType", ColumnTypeCode.Decimal},
            {"org.apache.cassandra.db.marshal.IntegerType", ColumnTypeCode.Varint},
            {"org.apache.cassandra.db.marshal.CounterColumnType", ColumnTypeCode.Counter}
        };

        private static readonly int SingleTypeNamesLength = SingleTypeNames.Keys.OrderByDescending(k => k.Length).First().Length;

        internal static byte[] GuidShuffle(byte[] b)
        {
            return new[] {b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]};
        }

        internal static int BytesToInt32(byte[] buffer, int idx)
        {
            return (buffer[idx] << 24)
                   | (buffer[idx + 1] << 16 & 0xFF0000)
                   | (buffer[idx + 2] << 8 & 0xFF00)
                   | (buffer[idx + 3] & 0xFF);
        }

        private static ushort BytesToUInt16(byte[] buffer, int idx)
        {
            return (ushort)((buffer[idx] << 8) | (buffer[idx + 1] & 0xFF));
        }

        private static short BytesToInt16(byte[] buffer, int idx)
        {
            return (short)((buffer[idx] << 8) | (buffer[idx + 1] & 0xFF));
        }

        private static long BytesToInt64(byte[] buffer, int idx)
        {
            return (long) (
                              (((ulong) buffer[idx] << 56) & 0xFF00000000000000)
                              | (((ulong) buffer[idx + 1] << 48) & 0xFF000000000000)
                              | (((ulong) buffer[idx + 2] << 40) & 0xFF0000000000)
                              | (((ulong) buffer[idx + 3] << 32) & 0xFF00000000)
                              | (((ulong) buffer[idx + 4] << 24) & 0xFF000000)
                              | (((ulong) buffer[idx + 5] << 16) & 0xFF0000)
                              | (((ulong) buffer[idx + 6] << 8) & 0xFF00)
                              | (((ulong) buffer[idx + 7]) & 0xFF)
                          );
        }

        private static DateTimeOffset BytesToDateTimeOffset(byte[] buffer)
        {
            return UnixStart.AddMilliseconds(BytesToInt64(buffer, 0));
        }

        private static byte[] DateTimeOffsetToBytes(DateTimeOffset dt)
        {
            return BeConverter.GetBytes(Convert.ToInt64(Math.Floor((dt - UnixStart).TotalMilliseconds)));
        }

        public static TimeSpan ToUnixTime(DateTimeOffset value)
        {
            return value - UnixStart;
        }

        /// <summary>
        /// Takes the raw bytes to reconstruct a CLR object.
        /// </summary>
        public static object Decode(int protocolVersion, byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo, Type cSharpType = null)
        {
            DecodeHandler handler;
            if (!Decoders.TryGetValue(typeCode, out handler))
            {
                throw new InvalidTypeException("No decoder defined for type code " + typeCode);
            }
            return handler(protocolVersion, typeInfo, buffer, cSharpType);
        }

        /// <summary>
        /// Gets the default CLR type for a given CQL type
        /// </summary>
        /// <exception cref="ArgumentException" />
        public static Type GetDefaultTypeFromCqlType(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            DefaultTypeFromCqlTypeDelegate clrTypeHandler;
            if (!DefaultTypes.TryGetValue(typeCode, out clrTypeHandler))
            {
                throw new ArgumentException("No handler defined for type " + typeCode);
            }
            return clrTypeHandler(typeInfo);
        }

        public static ColumnTypeCode GetColumnTypeCodeInfo(Type type, out IColumnInfo typeInfo)
        {
            typeInfo = null;
            ColumnTypeCode singleCqlType;
            if (DefaultSingleCqlTypes.TryGetValue(type, out singleCqlType))
            {
                return singleCqlType;
            }
            if (type.IsGenericType)
            {
                if (type.Name.Equals("Nullable`1"))
                {
                    return GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out typeInfo);
                }
                if (typeof (IEnumerable).IsAssignableFrom(type))
                {
                    if (type.GetInterface("ISet`1") != null)
                    {
                        IColumnInfo keyTypeInfo;
                        ColumnTypeCode keyTypeCode = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out keyTypeInfo);
                        typeInfo = new SetColumnInfo {KeyTypeCode = keyTypeCode, KeyTypeInfo = keyTypeInfo};
                        return ColumnTypeCode.Set;
                    }
                    if (type.GetInterface("IDictionary`2") != null || type.GetGenericTypeDefinition() == typeof(IDictionary<,>))
                    {
                        IColumnInfo keyTypeInfo;
                        ColumnTypeCode keyTypeCode = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out keyTypeInfo);
                        IColumnInfo valueTypeInfo;
                        ColumnTypeCode valueTypeCode = GetColumnTypeCodeInfo(type.GetGenericArguments()[1], out valueTypeInfo);
                        typeInfo = new MapColumnInfo
                        {
                            KeyTypeCode = keyTypeCode,
                            KeyTypeInfo = keyTypeInfo,
                            ValueTypeCode = valueTypeCode,
                            ValueTypeInfo = valueTypeInfo
                        };
                        return ColumnTypeCode.Map;
                    }
                    if (type.GetInterface("IEnumerable`1") != null || type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    {
                        IColumnInfo valueTypeInfo;
                        ColumnTypeCode valueTypeCode = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out valueTypeInfo);
                        typeInfo = new ListColumnInfo {ValueTypeCode = valueTypeCode, ValueTypeInfo = valueTypeInfo};
                        return ColumnTypeCode.List;
                    }
                }
                if (typeof(IStructuralComparable).IsAssignableFrom(type) && type.FullName.StartsWith("System.Tuple"))
                {
                    typeInfo = new TupleColumnInfo
                    {
                        Elements = type.GetGenericArguments().Select(t =>
                        {
                            IColumnInfo tupleSubTypeInfo;
                            var dataType = new ColumnDesc
                            {
                                TypeCode = GetColumnTypeCodeInfo(t, out tupleSubTypeInfo),
                                TypeInfo = tupleSubTypeInfo
                            };
                            return dataType;
                        }).ToList()
                    };

                    return ColumnTypeCode.Tuple;
                }
            }
            if (type.IsArray)
            {
                IColumnInfo valueTypeInfo;
                ColumnTypeCode valueTypeCode = GetColumnTypeCodeInfo(type.GetElementType(), out valueTypeInfo);
                typeInfo = new ListColumnInfo { ValueTypeCode = valueTypeCode, ValueTypeInfo = valueTypeInfo };
                return ColumnTypeCode.List;
            }

            //Determine if its a Udt type
            var udtMap = GetUdtMap(type);
            if (udtMap != null)
            {
                typeInfo = udtMap.Definition;
                return ColumnTypeCode.Udt;
            }

            throw new InvalidTypeException("Unknown Cassandra target type for CLR type " + type.FullName);
        }

        /// <summary>
        /// Takes an object and serializes it into bytes using the protocol format
        /// </summary>
        /// <exception cref="InvalidTypeException" />
        public static byte[] Encode(int protocolVersion, object value)
        {
            if (value == Unset.Value)
            {
                if (protocolVersion < 4)
                {
                    throw new InvalidTypeException("Unset is not supported by this Cassandra version");   
                }
                //Return a buffer that represents the value Unset
                return UnsetBuffer;
            }
            if (value == null)
            {
                return null;
            }
            IColumnInfo typeInfo;
            var typeCode = GetColumnTypeCodeInfo(value.GetType(), out typeInfo);
            EncodeHandler handler;
            if (!Encoders.TryGetValue(typeCode, out handler))
            {
                throw new InvalidTypeException("No encoder defined for type code " + typeCode);
            }
            return handler(protocolVersion, typeInfo, value);
        }

        internal static void CheckArgument(Type t, object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException();
            }
            if (!t.IsInstanceOfType(value))
            {
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] {t.FullName});
            }
        }

        internal static void CheckArgument<T>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            if (!(value is T))
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] {typeof (T).FullName});
        }

        private static void CheckArgument<T1, T2>(object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException();
            }
            if (!(value is T1 || value is T2))
            {
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] {typeof (T1).FullName, typeof (T2).FullName});
            }
        }

        public static object DecodeAscii(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return Encoding.ASCII.GetString(value);
        }

        public static byte[] EncodeAscii(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<string>(value);
            return Encoding.ASCII.GetBytes((string) value);
        }

        public static object DecodeBlob(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return value;
        }

        public static byte[] EncodeBlob(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[]) value;
        }

        public static object DecodeBigint(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return BytesToInt64(value, 0);
        }

        public static byte[] EncodeBigint(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<long>(value);
            return BeConverter.GetBytes((long) value);
        }

        public static object DecodeUuid(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return new Guid(GuidShuffle(value));
        }

        public static byte[] EncodeUuid(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid) value).ToByteArray());
        }

        public static object DecodeVarint(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return TypeAdapters.VarIntTypeAdapter.ConvertFrom(buffer);
        }

        private static Type GetDefaultTypeFromVarint(IColumnInfo typeInfo)
        {
            return TypeAdapters.VarIntTypeAdapter.GetDataType();
        }

        public static byte[] EncodeVarint(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            byte[] ret = TypeAdapters.VarIntTypeAdapter.ConvertTo(value);
            Array.Reverse(ret);
            return ret;
        }

        public static object DecodeSet(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is SetColumnInfo))
            {
                throw new DriverInternalError("Expected SetColumnInfo, obtained " + typeInfo.GetType());
            }
            var childTypeCode = ((SetColumnInfo) typeInfo).KeyTypeCode;
            var childTypeInfo = ((SetColumnInfo)typeInfo).KeyTypeInfo;
            var valueType = GetDefaultTypeFromCqlType(childTypeCode, childTypeInfo);
            var result = DecodeCollection(protocolVersion, valueType, childTypeCode, childTypeInfo, value);
            if (cSharpType == null)
            {
                return Utils.ToCollectionType(typeof(List<>), valueType, result);
            }
            //Is a generic type is expected, check if supported
            if (cSharpType.IsArray)
            {
                return result;
            }
            if (!cSharpType.IsGenericType)
            {
                return Utils.ToCollectionType(typeof (List<>), valueType, result);
            }
            if (cSharpType.GetGenericTypeDefinition() == typeof(SortedSet<>))
            {
                return Utils.ToCollectionType(typeof(SortedSet<>), valueType, result);
            }
            if (cSharpType.GetGenericTypeDefinition() == typeof(HashSet<>))
            {
                return Utils.ToCollectionType(typeof(HashSet<>), valueType, result);
            }
            return Utils.ToCollectionType(typeof(List<>), valueType, result);
        }

        /// <summary>
        /// Decodes length for collection types depending on the protocol version
        /// </summary>
        private static int DecodeCollectionLength(int protocolVersion, byte[] buffer, ref int index)
        {
            int result;
            if (protocolVersion < 3)
            {
                //length is a short
                result = BytesToUInt16(buffer, index);
                index += 2;
            }
            else
            {
                //length is expressed in int
                result = BytesToInt32(buffer, index);
                index += 4;
            }
            return result;
        }

        private static Array DecodeCollection(int protocolVersion, Type childType, ColumnTypeCode childTypeCode, IColumnInfo childTypeInfo, byte[] value)
        {
            var index = 0;
            var count = DecodeCollectionLength(protocolVersion, value, ref index);
            var result = Array.CreateInstance(childType, count);
            for (var i = 0; i < count; i++)
            {
                var valueBufferLength = DecodeCollectionLength(protocolVersion, value, ref index);
                var valBuf = new byte[valueBufferLength];
                Buffer.BlockCopy(value, index, valBuf, 0, valueBufferLength);
                index += valueBufferLength;
                result.SetValue(Decode(protocolVersion, valBuf, childTypeCode, childTypeInfo), i);
            }
            return result;
        }

        private static Type GetDefaultTypeFromSet(IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is SetColumnInfo))
            {
                throw new InvalidTypeException("Expected SetColumnInfo, obtained " + typeInfo.GetType());
            }
            var innerTypeCode = (typeInfo as SetColumnInfo).KeyTypeCode;
            var innerTypeInfo = (typeInfo as SetColumnInfo).KeyTypeInfo;
            var valueType = GetDefaultTypeFromCqlType(innerTypeCode, innerTypeInfo);
            var openType = typeof (IEnumerable<>);
            return openType.MakeGenericType(valueType);
        }

        public static object DecodeTimestamp(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (cSharpType == null || cSharpType == typeof(object) || cSharpType == typeof(DateTimeOffset))
            {
                return BytesToDateTimeOffset(value);
            }
            return BytesToDateTimeOffset(value).DateTime;
        }

        public static byte[] EncodeTimestamp(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<DateTimeOffset, DateTime>(value);
            if (value is DateTimeOffset)
                return DateTimeOffsetToBytes((DateTimeOffset) value);
            var dt = (DateTime) value;
            // need to treat "Unspecified" as UTC (+0) not the default behavior of DateTimeOffset which treats as Local Timezone
            // because we are about to do math against EPOCH which must align with UTC. 
            // If we don't, then the value saved will be shifted by the local timezone when retrieved back out as DateTime.
            return DateTimeOffsetToBytes(dt.Kind == DateTimeKind.Unspecified
                                             ? new DateTimeOffset(dt, TimeSpan.Zero)
                                             : new DateTimeOffset(dt));
        }

        public static object DecodeTimeuuid(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var decodedValue = new Guid(GuidShuffle(value));
            if (cSharpType == typeof (TimeUuid))
            {
                return (TimeUuid) decodedValue;
            }
            return decodedValue;
        }

        public static byte[] EncodeTimeuuid(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            if (value is TimeUuid)
            {
                value = ((TimeUuid) value).ToGuid();
            }
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid) value).ToByteArray());
        }

        public static object DecodeMap(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is MapColumnInfo))
            {
                throw new InvalidTypeException("Expected MapColumnInfo, obtained " + typeInfo.GetType());
            }
            var keyTypecode = (typeInfo as MapColumnInfo).KeyTypeCode;
            var keyTypeinfo = (typeInfo as MapColumnInfo).KeyTypeInfo;
            var valueTypecode = (typeInfo as MapColumnInfo).ValueTypeCode;
            var valueTypeinfo = (typeInfo as MapColumnInfo).ValueTypeInfo;
            var keyType = GetDefaultTypeFromCqlType(keyTypecode, keyTypeinfo);
            var valueType = GetDefaultTypeFromCqlType(valueTypecode, valueTypeinfo);
            var index = 0;
            var count = DecodeCollectionLength(protocolVersion, value, ref index);
            var openType = typeof (SortedDictionary<,>);
            var dicType = openType.MakeGenericType(keyType, valueType);
            var result = (IDictionary) Activator.CreateInstance(dicType);
            for (var i = 0; i < count; i++)
            {
                var keyBufLen = DecodeCollectionLength(protocolVersion, value, ref index);
                var keyBuf = new byte[keyBufLen];
                Buffer.BlockCopy(value, index, keyBuf, 0, keyBufLen);
                index += keyBufLen;

                var valueBufLen = DecodeCollectionLength(protocolVersion, value, ref index);
                var valueBuf = new byte[valueBufLen];
                Buffer.BlockCopy(value, index, valueBuf, 0, valueBufLen);
                index += valueBufLen;

                result.Add(
                    Decode(protocolVersion, keyBuf, keyTypecode, keyTypeinfo),
                    Decode(protocolVersion, valueBuf, valueTypecode, valueTypeinfo)
                    );
            }
            return result;
        }

        private static Type GetDefaultTypeFromMap(IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is MapColumnInfo))
            {
                throw new InvalidTypeException("Expected MapColumnInfo, obtained " + typeInfo.GetType());
            }
            var keyTypecode = (typeInfo as MapColumnInfo).KeyTypeCode;
            var keyTypeinfo = (typeInfo as MapColumnInfo).KeyTypeInfo;
            var valueTypecode = (typeInfo as MapColumnInfo).ValueTypeCode;
            var valueTypeinfo = (typeInfo as MapColumnInfo).ValueTypeInfo;
            var keyType = GetDefaultTypeFromCqlType(keyTypecode, keyTypeinfo);
            var valueType = GetDefaultTypeFromCqlType(valueTypecode, valueTypeinfo);

            var openType = typeof (IDictionary<,>);
            return openType.MakeGenericType(keyType, valueType);
        }

        public static byte[] EncodeMap(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            //[n] indicating the size of the map, followed by n entries. 
            //Each entry is composed of two [bytes] representing the key and the value of the entry map.
            //The length can be expressed in 2 or 4 bytes depending on the protocol v3

            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is MapColumnInfo))
            {
                throw new InvalidTypeException("Expected MapColumnInfo, obtained " + typeInfo.GetType());
            }
            var dicType = GetDefaultTypeFromMap(typeInfo);
            CheckArgument(dicType, value);

            var dictionary = (IDictionary)value;
            var keyBuffers = new List<byte[]>(dictionary.Count);
            var valueBuffers = new List<byte[]>(dictionary.Count);
            var byteLength = 0;
            foreach (DictionaryEntry item in dictionary)
            {
                var itemKeyBuffer = Encode(protocolVersion, item.Key);
                if (item.Value == null)
                {
                    throw new ArgumentNullException("key:" + item.Key, "Null values are not supported inside collections");
                }
                var itemValueBuffer = Encode(protocolVersion, item.Value);
                keyBuffers.Add(itemKeyBuffer);
                valueBuffers.Add(itemValueBuffer);
                byteLength += itemKeyBuffer.Length + itemValueBuffer.Length;
            }

            var index = 0;
            var itemsLength = EncodeCollectionLength(protocolVersion, keyBuffers.Count);
            var collectionLengthSize = itemsLength.Length;
            byteLength += keyBuffers.Count * collectionLengthSize * 2 + collectionLengthSize;
            var result = new byte[byteLength];

            Buffer.BlockCopy(itemsLength, 0, result, 0, collectionLengthSize);
            index += collectionLengthSize;
            //For each item, encode the length, the key bytes and the value bytes
            for (var i = 0; i < keyBuffers.Count; i++)
            {
                //write key
                var itemKeyBuffer = keyBuffers[i];
                var itemKeyLength = EncodeCollectionLength(protocolVersion, itemKeyBuffer.Length);
                Buffer.BlockCopy(itemKeyLength, 0, result, index, collectionLengthSize);
                index += collectionLengthSize;
                Buffer.BlockCopy(itemKeyBuffer, 0, result, index, itemKeyBuffer.Length);
                index += itemKeyBuffer.Length;

                //write value
                var itemValueBuffer = valueBuffers[i];
                var itemValueLength = EncodeCollectionLength(protocolVersion, itemValueBuffer.Length);
                Buffer.BlockCopy(itemValueLength, 0, result, index, collectionLengthSize);
                index += collectionLengthSize;
                Buffer.BlockCopy(itemValueBuffer, 0, result, index, itemValueBuffer.Length);
                index += itemValueBuffer.Length;
            }
            return result;
        }

        public static object DecodeText(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static byte[] EncodeText(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string) value);
        }

        public static object DecodeVarchar(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static byte[] EncodeVarchar(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string) value);
        }

        public static object DecodeList(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is ListColumnInfo))
            {
                throw new InvalidTypeException("Expected ListColumnInfo typeInfo, obtained " + typeInfo.GetType());
            }
            var childTypeCode = ((ListColumnInfo) typeInfo).ValueTypeCode;
            var childTypeInfo = ((ListColumnInfo) typeInfo).ValueTypeInfo;
            var valueType = GetDefaultTypeFromCqlType(childTypeCode, childTypeInfo);
            var result = DecodeCollection(protocolVersion, valueType, childTypeCode, childTypeInfo, value);
            if (cSharpType != null && cSharpType.IsArray)
            {
                return result;
            }
            return Utils.ToCollectionType(typeof(List<>), valueType, result);
        }

        private static Type GetDefaultTypeFromList(IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is ListColumnInfo))
            {
                throw new InvalidTypeException("Expected ListColumnInfo typeInfo, obtained " + typeInfo.GetType());
            }
            var listTypecode = (typeInfo as ListColumnInfo).ValueTypeCode;
            var listTypeinfo = (typeInfo as ListColumnInfo).ValueTypeInfo;
            var valueType = GetDefaultTypeFromCqlType(listTypecode, listTypeinfo);
            var openType = typeof (IEnumerable<>);
            return openType.MakeGenericType(valueType);
        }

        public static byte[] EncodeList(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            var listType = GetDefaultTypeFromList(typeInfo);
            CheckArgument(listType, value);
            return EncodeCollection(protocolVersion, (IEnumerable)value);
        }

        public static byte[] EncodeSet(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            var setType = GetDefaultTypeFromSet(typeInfo);
            CheckArgument(setType, value);
            return EncodeCollection(protocolVersion, (IEnumerable)value);
        }

        /// <summary>
        /// Encodes a list or a set into a protocol encoded bytes
        /// </summary>
        public static byte[] EncodeCollection(int protocolVersion, IEnumerable value)
        {
            //protocol format: [n items][bytes_1][bytes_n]
            //where the amount of bytes to express the length are 2 or 4 depending on the protocol version
            var bufferList = new List<byte[]>();
            var byteLength = 0;
            foreach (var item in value)
            {
                if (item == null)
                {
                    throw new ArgumentNullException(null, "Null values are not supported inside collections");   
                }
                var buf = Encode(protocolVersion, item);
                bufferList.Add(buf);
                byteLength += buf.Length;
            }
            var index = 0;
            var itemsLength = EncodeCollectionLength(protocolVersion, bufferList.Count);
            var collectionLengthSize = itemsLength.Length;
            byteLength += (bufferList.Count + 1) * collectionLengthSize;
            var result = new byte[byteLength];

            Buffer.BlockCopy(itemsLength, 0, result, 0, collectionLengthSize);
            index += collectionLengthSize;
            //For each item, encode the length and the byte value
            foreach (var buf in bufferList)
            {
                var bufferItemLength = EncodeCollectionLength(protocolVersion, buf.Length);
                Buffer.BlockCopy(bufferItemLength, 0, result, index, collectionLengthSize);
                index += collectionLengthSize;

                Buffer.BlockCopy(buf, 0, result, index, buf.Length);
                index += buf.Length;
            }
            return result;
        }

        public static byte[] EncodeUdt(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is UdtColumnInfo))
            {
                throw new ArgumentException("Expected UdtColumn typeInfo, obtained " + typeInfo.GetType());
            }
            var map = GetUdtMap((typeInfo as UdtColumnInfo).Name);
            var bufferList = new List<byte[]>();
            var bufferLength = 0;
            foreach (var field in map.Definition.Fields)
            {
                object fieldValue = null;
                var prop = map.GetPropertyForUdtField(field.Name);
                if (prop != null)
                {
                    fieldValue = prop.GetValue(value, null);
                }
                var itemBuffer = Encode(protocolVersion, fieldValue);
                bufferList.Add(itemBuffer);
                if (fieldValue != null)
                {
                    bufferLength += itemBuffer.Length;   
                }
            }
            return EncodeBufferList(bufferList, bufferLength);
        }

        public static byte[] EncodeTuple(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            var tupleType = value.GetType();
            var subtypes = tupleType.GetGenericArguments();
            var bufferList = new List<byte[]>();
            var bufferLength = 0;
            for (var i = 1; i <= subtypes.Length; i++)
            {
                var prop = tupleType.GetProperty("Item" + i);
                if (prop != null)
                {
                    var buffer = Encode(protocolVersion, prop.GetValue(value, null));
                    bufferList.Add(buffer);
                    if (buffer != null)
                    {
                        bufferLength += buffer.Length;   
                    }
                }
            }
            return EncodeBufferList(bufferList, bufferLength);
        }

        private static byte[] EncodeBufferList(List<byte[]> bufferList, int bufferLength)
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
        /// Uses 2 or 4 bytes to represent the length in bytes
        /// </summary>
        private static byte[] EncodeCollectionLength(int protocolVersion, int value)
        {
            if (protocolVersion < 3)
            {
                return BeConverter.GetBytes((short)value);
            }
            return BeConverter.GetBytes(value);
        }

        public static object DecodeInet(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (value.Length == 4 || value.Length == 16)
            {
                return new IPAddress(value);
            }
            throw new DriverInternalError("Invalid length of Inet Addr");
        }

        public static byte[] EncodeInet(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<IPAddress>(value);
            // ReSharper disable once PossibleNullReferenceException
            return (value as IPAddress).GetAddressBytes();
        }

        public static object DecodeCounter(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return BytesToInt64(buffer, 0);
        }

        public static byte[] EncodeCounter(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<long>(value);
            return BeConverter.GetBytes((long) value);
        }

        public static object DecodeDouble(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return BitConverter.ToDouble(new [] { value[7], value[6], value[5], value[4], value[3], value[2], value[1], value[0] }, 0);
        }

        public static byte[] EncodeDouble(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<double>(value);
            byte[] ret = BitConverter.GetBytes((double) value);
            Array.Reverse(ret);
            return ret;
        }

        public static object DecodeInt(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return BytesToInt32(buffer, 0);
        }

        public static object DecodeDate(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            var days = unchecked((uint)((buffer[0] << 24)
                   | (buffer[1] << 16)
                   | (buffer[2] << 8)
                   | (buffer[3])));
            return new LocalDate(days);
        }

        public static object DecodeTime(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return new LocalTime(BytesToInt64(buffer, 0));
        }

        public static object DecodeShort(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return BytesToInt16(buffer, 0);
        }

        public static object DecodeSByte(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return unchecked((sbyte)buffer[0]);
        }

        public static byte[] EncodeInt(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<int>(value);
            return BeConverter.GetBytes((int)value);
        }

        public static byte[] EncodeDate(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<LocalDate>(value);
            var val = ((LocalDate) value).DaysSinceEpochCentered;
            return new []
            {
                (byte) ((val & 0xFF000000) >> 24),
                (byte) ((val & 0xFF0000) >> 16),
                (byte) ((val & 0xFF00) >> 8),
                (byte) (val & 0xFF)
            };
        }

        public static byte[] EncodeTime(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<LocalTime>(value);
            return BeConverter.GetBytes(((LocalTime)value).TotalNanoseconds);
        }

        public static byte[] EncodeShort(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<short>(value);
            return BeConverter.GetBytes((short)value);
        }

        public static byte[] EncodeSByte(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<sbyte>(value);
            return new[] {unchecked((byte)(sbyte)value)};
        }

        public static object DecodeFloat(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return BitConverter.ToSingle(buffer, 0);
        }

        public static byte[] EncodeFloat(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<float>(value);
            byte[] ret = BitConverter.GetBytes((float) value);
            Array.Reverse(ret);
            return ret;
        }

        public static object DecodeCustom(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo is CustomColumnInfo)
            {
                var customInfo = (CustomColumnInfo) typeInfo;
                if (customInfo.CustomTypeName != null && customInfo.CustomTypeName.StartsWith(UdtTypeName))
                {
                    var dataType = ParseDataType(customInfo.CustomTypeName);
                    return DecodeUdtMapping(protocolVersion, dataType.Keyspace + "." + dataType.Name, value);
                }
            }
            return value;
        }

        public static object DecodeUdt(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is UdtColumnInfo))
            {
                throw new ArgumentException("Expected UdtColumn typeInfo, obtained " + typeInfo.GetType());
            }
            return DecodeUdtMapping(protocolVersion, (typeInfo as UdtColumnInfo).Name, value);
        }

        /// <exception cref="ArgumentException" />
        private static Type GetDefaultTypeFromUdt(IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is UdtColumnInfo))
            {
                throw new ArgumentException("Expected UdtColumn typeInfo, obtained " + typeInfo.GetType());
            }
            var map = GetUdtMap(((UdtColumnInfo) typeInfo).Name);
            if (map == null)
            {
                return typeof (byte[]);
            }
            return map.NetType;
        }

        /// <exception cref="ArgumentException" />
        private static Type GetDefaultTypeFromTuple(IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is TupleColumnInfo))
            {
                throw new ArgumentException("Expected TupleColumnInfo typeInfo, obtained " + typeInfo.GetType());
            }
            var tupleInfo = (TupleColumnInfo) typeInfo;
            Type genericTupleType;
            switch (tupleInfo.Elements.Count)
            {
                case 1:
                    genericTupleType = typeof(Tuple<>);
                    break;
                case 2:
                    genericTupleType = typeof(Tuple<,>);
                    break;
                case 3:
                    genericTupleType = typeof(Tuple<,,>);
                    break;
                case 4:
                    genericTupleType = typeof(Tuple<,,,>);
                    break;
                case 5:
                    genericTupleType = typeof(Tuple<,,,,>);
                    break;
                case 6:
                    genericTupleType = typeof(Tuple<,,,,,>);
                    break;
                case 7:
                    genericTupleType = typeof(Tuple<,,,,,,>);
                    break;
                default:
                    return typeof(byte[]);
            }

            return genericTupleType.MakeGenericType(tupleInfo.Elements.Select(s => GetDefaultTypeFromCqlType(s.TypeCode, s.TypeInfo)).ToArray());
        }

        private static object DecodeUdtMapping(int protocolVersion, string udtName, byte[] value)
        {
            var map = GetUdtMap(udtName);
            if (map == null)
            {
                return value;
            }
            if (map.Definition == null)
            {
                throw new ArgumentException("Udt mapping does not contain the Udt definition");
            }
            var valuesList = new List<object>();
            var stream = new MemoryStream(value, false);
            var reader = new BEBinaryReader(stream);
            foreach (var field in map.Definition.Fields)
            {
                if (stream.Position < value.Length - 1)
                {
                    var length = reader.ReadInt32();
                    if (length < 0)
                    {
                        valuesList.Add(null);
                    }
                    else
                    {
                        var buffer = new byte[length];
                        reader.Read(buffer, 0, length);
                        valuesList.Add(Decode(protocolVersion, buffer, field.TypeCode, field.TypeInfo));
                    }
                }
            }
            return map.ToObject(valuesList);
        }

        public static object DecodeTuple(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is TupleColumnInfo))
            {
                throw new ArgumentException("Expected UdtColumn typeInfo, obtained " + typeInfo.GetType());
            }
            Type tupleType = null;
            if (cSharpType != null &&
                cSharpType.IsSerializable &&
                typeof(IStructuralComparable).IsAssignableFrom(cSharpType) &&
                cSharpType.FullName.StartsWith("System.Tuple"))
            {
                tupleType = cSharpType;
            }
            var tupleInfo = (TupleColumnInfo) typeInfo;
            var valuesList = new List<object>();
            var stream = new MemoryStream(value, false);
            var reader = new BEBinaryReader(stream);
            foreach (var element in tupleInfo.Elements)
            {
                if (stream.Position >= value.Length - 1)
                {
                    break;
                }
                var length = reader.ReadInt32();
                if (length < 0)
                {
                    valuesList.Add(null);
                }
                else
                {
                    var buffer = new byte[length];
                    reader.Read(buffer, 0, length);
                    valuesList.Add(Decode(protocolVersion, buffer, element.TypeCode, element.TypeInfo));
                }
            }

            if (tupleType == null)
            {
                tupleType = GetDefaultTypeFromTuple(tupleInfo);
            }
            else if (tupleType.GetGenericArguments().Length > valuesList.Count)
            {
                valuesList.AddRange(Enumerable.Repeat<object>(null, tupleType.GetGenericArguments().Length - valuesList.Count));
            }
            return Activator.CreateInstance(tupleType, valuesList.ToArray());
        }

        private static Type GetDefaultTypeFromCustom(IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is CustomColumnInfo))
            {
                throw new ArgumentException("Expected CustomColumnInfo typeInfo, obtained " + typeInfo.GetType());
            }

            var customName = ((CustomColumnInfo) typeInfo).CustomTypeName;
            if (customName == null || !customName.StartsWith(UdtTypeName))
            {
                return typeof (byte[]);
            }
            var dataType = ParseDataType(customName);
            var map = GetUdtMap(dataType.Keyspace + "." + dataType.Name);
            if (map == null)
            {
                throw new InvalidTypeException("No mapping defined for udt type " + customName);
            }
            return map.NetType;
        }

        public static byte[] EncodeCustom(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[]) value;
        }

        public static object DecodeBoolean(int protocolVersion, IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return buffer[0] == 1;
        }

        public static byte[] EncodeBoolean(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            CheckArgument<bool>(value);
            var buffer = new byte[1];
            buffer[0] = ((bool) value) ? (byte) 0x01 : (byte) 0x00;
            return buffer;
        }

        public static object DecodeDecimal(int protocolVersion, IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            return TypeAdapters.DecimalTypeAdapter.ConvertFrom(buffer);
        }

        private static Type GetDefaultTypeFromDecimal(IColumnInfo typeInfo)
        {
            return TypeAdapters.DecimalTypeAdapter.GetDataType();
        }

        public static byte[] EncodeDecimal(int protocolVersion, IColumnInfo typeInfo, object value)
        {
            byte[] ret = TypeAdapters.DecimalTypeAdapter.ConvertTo(value);
            return ret;
        }

        private delegate Type DefaultTypeFromCqlTypeDelegate(IColumnInfo typeInfo);
        
        /// <summary>
        /// Parses a given Cassandra type name to get the data type information
        /// </summary>
        /// <exception cref="ArgumentException" />
        internal static ColumnDesc ParseDataType(string typeName, int startIndex = 0, int length = 0)
        {
            var dataType = new ColumnDesc();
            if (length == 0)
            {
                length = typeName.Length;
            }
            if (length > ReversedTypeName.Length && typeName.Substring(startIndex, ReversedTypeName.Length) == ReversedTypeName)
            {
                //We don't care if the clustering order is reversed
                startIndex += ReversedTypeName.Length + 1;
                length -= ReversedTypeName.Length + 2;
            }
            if (length > FrozenTypeName.Length && typeName.Substring(startIndex, FrozenTypeName.Length) == FrozenTypeName)
            {
                //Remove the frozen
                //We can later store that it is frozen, if needed
                startIndex += FrozenTypeName.Length + 1;
                length -= FrozenTypeName.Length + 2;
            }
            //Quick check if its a single type
            if (length <= SingleTypeNamesLength)
            {
                ColumnTypeCode typeCode;
                if (startIndex > 0)
                {
                    typeName = typeName.Substring(startIndex, length);
                }
                if (SingleTypeNames.TryGetValue(typeName, out typeCode))
                {
                    dataType.TypeCode = typeCode;
                    return dataType;
                }
                throw GetTypeException(typeName);
            }
            if (typeName.Substring(startIndex, ListTypeName.Length) == ListTypeName)
            {
                //Its a list
                //org.apache.cassandra.db.marshal.ListType(innerType)
                //move cursor across the name and bypass the parenthesis
                startIndex += ListTypeName.Length + 1;
                length -= ListTypeName.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length);
                if (innerTypes.Count != 1)
                {
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.List;
                var subType = ParseDataType(innerTypes[0]);
                dataType.TypeInfo = new ListColumnInfo()
                {
                    ValueTypeCode = subType.TypeCode,
                    ValueTypeInfo = subType.TypeInfo
                };
                return dataType;
            }
            if (typeName.Substring(startIndex, SetTypeName.Length) == SetTypeName)
            {
                //Its a set
                //org.apache.cassandra.db.marshal.SetType(innerType)
                //move cursor across the name and bypass the parenthesis
                startIndex += SetTypeName.Length + 1;
                length -= SetTypeName.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length);
                if (innerTypes.Count != 1)
                {
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Set;
                var subType = ParseDataType(innerTypes[0]);
                dataType.TypeInfo = new SetColumnInfo()
                {
                    KeyTypeCode = subType.TypeCode,
                    KeyTypeInfo = subType.TypeInfo
                };
                return dataType;
            }
            if (typeName.Substring(startIndex, MapTypeName.Length) == MapTypeName)
            {
                //org.apache.cassandra.db.marshal.MapType(keyType,valueType)
                //move cursor across the name and bypass the parenthesis
                startIndex += MapTypeName.Length + 1;
                length -= MapTypeName.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length);
                //It should contain the key and value types
                if (innerTypes.Count != 2)
                {
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Map;
                var keyType = ParseDataType(innerTypes[0]);
                var valueType = ParseDataType(innerTypes[1]);
                dataType.TypeInfo = new MapColumnInfo()
                {
                    KeyTypeCode = keyType.TypeCode,
                    KeyTypeInfo = keyType.TypeInfo,
                    ValueTypeCode = valueType.TypeCode,
                    ValueTypeInfo = valueType.TypeInfo
                };
                return dataType;
            }
            if (typeName.Substring(startIndex, UdtTypeName.Length) == UdtTypeName)
            {
                //move cursor across the name and bypass the parenthesis
                startIndex += UdtTypeName.Length + 1;
                length -= UdtTypeName.Length + 2;
                var udtParams = ParseParams(typeName, startIndex, length);
                if (udtParams.Count < 2)
                {
                    //It should contain at least the keyspace, name of the udt and a type
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Udt;
                dataType.Keyspace = udtParams[0];
                dataType.Name = HexToUtf8(udtParams[1]);
                var udtInfo = new UdtColumnInfo(dataType.Keyspace + "." + dataType.Name);
                for (var i = 2; i < udtParams.Count; i++)
                {
                    var p = udtParams[i];
                    var separatorIndex = p.IndexOf(':');
                    var c = ParseDataType(p, separatorIndex + 1, p.Length - (separatorIndex + 1));
                    c.Name = HexToUtf8(p.Substring(0, separatorIndex));
                    udtInfo.Fields.Add(c);
                }
                dataType.TypeInfo = udtInfo;
                return dataType;
            }
            if (typeName.Substring(startIndex, TupleTypeName.Length) == TupleTypeName)
            {
                //move cursor across the name and bypass the parenthesis
                startIndex += TupleTypeName.Length + 1;
                length -= TupleTypeName.Length + 2;
                var tupleParams = ParseParams(typeName, startIndex, length);
                if (tupleParams.Count < 1)
                {
                    //It should contain at least the keyspace, name of the udt and a type
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Tuple;
                var tupleInfo = new TupleColumnInfo();
                foreach (var subTypeName in tupleParams)
                {
                    tupleInfo.Elements.Add(ParseDataType(subTypeName));
                }
                dataType.TypeInfo = tupleInfo;
                return dataType;
            }
            throw GetTypeException(typeName);
        }

        /// <summary>
        /// Converts a hex string to utf8 string
        /// </summary>
        private static string HexToUtf8(string hexString)
        {
            var bytes = Enumerable.Range(0, hexString.Length)
                 .Where(x => x % 2 == 0)
                 .Select(x => Convert.ToByte(hexString.Substring(x, 2), 16))
                 .ToArray();
            return Encoding.UTF8.GetString(bytes);
        }

        /// <summary>
        /// Parses comma delimited type parameters
        /// </summary>
        /// <returns></returns>
        private static List<string> ParseParams(string value, int startIndex, int length)
        {
            var types = new List<string>();
            var paramStart = startIndex;
            var level = 0;
            for (var i = startIndex; i < startIndex + length; i++)
            {
                var c = value[i];
                if (c == '(')
                {
                    level++;
                }
                if (c == ')')
                {
                    level--;
                }
                if (level == 0 && c == ',')
                {
                    types.Add(value.Substring(paramStart, i - paramStart));
                    paramStart = i + 1;
                }
            }
            //Add the last one
            types.Add(value.Substring(paramStart, length - (paramStart - startIndex)));
            return types;
        }

        private static Exception GetTypeException(string typeName)
        {
            return new ArgumentException(String.Format("Not a valid type {0}", typeName));
        }

        /// <summary>
        /// Sets a Udt map for a given Udt name
        /// </summary>
        /// <param name="name">Fully qualified udt name case sensitive (keyspace.udtName)</param>
        /// <param name="map"></param>
        public static void SetUdtMap(string name, UdtMap map)
        {
            UdtMapsByName.AddOrUpdate(name, map, (k, oldValue) => map);
            UdtMapsByClrType.AddOrUpdate(map.NetType, map, (k, oldValue) => map);
        }

        /// <summary>
        /// Gets a UdtMap by fully qualified name.
        /// </summary>
        /// <param name="name">keyspace.udtName</param>
        /// <returns>Null if not found</returns>
        public static UdtMap GetUdtMap(string name)
        {
            UdtMap map;
            UdtMapsByName.TryGetValue(name, out map);
            return map;
        }

        /// <summary>
        /// Gets a UdtMap by fully qualified name.
        /// </summary>
        /// <returns>Null if not found</returns>
        public static UdtMap GetUdtMap(Type type)
        {
            UdtMap map;
            UdtMapsByClrType.TryGetValue(type, out map);
            return map;
        }

        /// <summary>
        /// Performs a lightweight validation to determine if the source type and target type matches.
        /// It isn't more invasive to support crazy uses of the driver, like direct inputs of blobs and all that. (backward compatibility)
        /// </summary>
        public static bool IsAssignableFrom(CqlColumn column, object value)
        {
            if (value == null || value is byte[])
            {
                return true;
            }
            var type = value.GetType();
            ColumnTypeCode cqlType;
            if (DefaultSingleCqlTypes.TryGetValue(type, out cqlType))
            {
                //Its a single type, if the types match -> go ahead
                if (cqlType == column.TypeCode) return true;
                //Only int32 and blobs are valid cql ints
                if (column.TypeCode == ColumnTypeCode.Int) return false;
                //Only double, longs and blobs are valid cql double
                if (column.TypeCode == ColumnTypeCode.Double && !(value is Int64)) return false;
                //The rest of the single values are not evaluated
                return true;
            }
            if (column.TypeCode == ColumnTypeCode.List || column.TypeCode == ColumnTypeCode.Set)
            {
                return value is IEnumerable;
            }
            if (column.TypeCode == ColumnTypeCode.Map)
            {
                return value is IDictionary;
            }
            return true;
        }
    }
}
