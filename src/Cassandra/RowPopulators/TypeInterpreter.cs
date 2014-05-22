//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Text;

namespace Cassandra
{
    internal delegate object CqlConvertDelegate(IColumnInfo typeInfo, byte[] buffer, Type cSharpType);
    internal delegate byte[] InvCqlConvertDelegate(IColumnInfo typeInfo, object value);

    internal class TypeInterpreter
    {
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        private static readonly Dictionary<Type, byte> MapTypeToCode = new Dictionary<Type, byte>();

        /// <summary>
        /// Decoders by type code
        /// </summary>
        private static readonly Dictionary<ColumnTypeCode, InvCqlConvertDelegate> Encoders = new Dictionary<ColumnTypeCode, InvCqlConvertDelegate>()
        {
            {ColumnTypeCode.Ascii,        InvConvertFromAscii},
            {ColumnTypeCode.Bigint,       InvConvertFromBigint},
            {ColumnTypeCode.Blob,         InvConvertFromBlob},
            {ColumnTypeCode.Boolean,      InvConvertFromBoolean},
            {ColumnTypeCode.Counter,      InvConvertFromCounter},
            {ColumnTypeCode.Custom,       InvConvertFromCustom},
            {ColumnTypeCode.Double,       InvConvertFromDouble},
            {ColumnTypeCode.Float,        InvConvertFromFloat},
            {ColumnTypeCode.Int,          InvConvertFromInt},
            {ColumnTypeCode.Text,         InvConvertFromText},
            {ColumnTypeCode.Timestamp,    InvConvertFromTimestamp},
            {ColumnTypeCode.Uuid,         InvConvertFromUuid},
            {ColumnTypeCode.Varchar,      InvConvertFromVarchar},
            {ColumnTypeCode.Timeuuid,     InvConvertFromTimeuuid},
            {ColumnTypeCode.Inet,         InvConvertFromInet},
            {ColumnTypeCode.List,         InvConvertFromList},
            {ColumnTypeCode.Map,          InvConvertFromMap},
            {ColumnTypeCode.Set,          InvConvertFromSet},
            {ColumnTypeCode.Decimal,      InvConvertFromDecimal},
            {ColumnTypeCode.Varint,       InvConvertFromVarint}
        };

        /// <summary>
        /// Decoders by type code
        /// </summary>
        private static readonly Dictionary<ColumnTypeCode, CqlConvertDelegate> Decoders = new Dictionary<ColumnTypeCode, CqlConvertDelegate>()
        {
            {ColumnTypeCode.Ascii,        ConvertFromAscii},
            {ColumnTypeCode.Bigint,       ConvertFromBigint},
            {ColumnTypeCode.Blob,         ConvertFromBlob},
            {ColumnTypeCode.Boolean,      ConvertFromBoolean},
            {ColumnTypeCode.Counter,      ConvertFromCounter},
            {ColumnTypeCode.Custom,       ConvertFromCustom},
            {ColumnTypeCode.Double,       ConvertFromDouble},
            {ColumnTypeCode.Float,        ConvertFromFloat},
            {ColumnTypeCode.Int,          ConvertFromInt},
            {ColumnTypeCode.Text,         ConvertFromText},
            {ColumnTypeCode.Timestamp,    ConvertFromTimestamp},
            {ColumnTypeCode.Uuid,         ConvertFromUuid},
            {ColumnTypeCode.Varchar,      ConvertFromVarchar},
            {ColumnTypeCode.Timeuuid,     ConvertFromTimeuuid},
            {ColumnTypeCode.Inet,         ConvertFromInet},
            {ColumnTypeCode.List,         ConvertFromList},
            {ColumnTypeCode.Map,          ConvertFromMap},
            {ColumnTypeCode.Set,          ConvertFromSet},
            {ColumnTypeCode.Decimal,      ConvertFromDecimal},
            {ColumnTypeCode.Varint,       ConvertFromVarint}
        };

        /// <summary>
        /// Default CLR type by type code
        /// </summary>
        private static readonly Dictionary<ColumnTypeCode, GetDefaultTypeFromCqlTypeDel> DefaultTypes = new Dictionary<ColumnTypeCode, GetDefaultTypeFromCqlTypeDel>()
        {
            {ColumnTypeCode.Ascii,        GetDefaultTypeFromAscii},
            {ColumnTypeCode.Bigint,       GetDefaultTypeFromBigint},
            {ColumnTypeCode.Blob,         GetDefaultTypeFromBlob},
            {ColumnTypeCode.Boolean,      GetDefaultTypeFromBoolean},
            {ColumnTypeCode.Counter,      GetDefaultTypeFromCounter},
            {ColumnTypeCode.Custom,       GetDefaultTypeFromCustom},
            {ColumnTypeCode.Double,       GetDefaultTypeFromDouble},
            {ColumnTypeCode.Float,        GetDefaultTypeFromFloat},
            {ColumnTypeCode.Int,          GetDefaultTypeFromInt},
            {ColumnTypeCode.Text,         GetDefaultTypeFromText},
            {ColumnTypeCode.Timestamp,    GetDefaultTypeFromTimestamp},
            {ColumnTypeCode.Uuid,         GetDefaultTypeFromUuid},
            {ColumnTypeCode.Varchar,      GetDefaultTypeFromVarchar},
            {ColumnTypeCode.Timeuuid,     GetDefaultTypeFromTimeuuid},
            {ColumnTypeCode.Inet,         GetDefaultTypeFromInet},
            {ColumnTypeCode.List,         GetDefaultTypeFromList},
            {ColumnTypeCode.Map,          GetDefaultTypeFromMap},
            {ColumnTypeCode.Set,          GetDefaultTypeFromSet},
            {ColumnTypeCode.Decimal,      GetDefaultTypeFromDecimal},
            {ColumnTypeCode.Varint,       GetDefaultTypeFromVarint}
        };

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
            return (ushort) ((buffer[idx] << 8) | (buffer[idx + 1] & 0xFF));
        }

        private static byte[] Int32ToBytes(int value)
        {
            return new[]
            {
                (byte) ((value & 0xFF000000) >> 24),
                (byte) ((value & 0xFF0000) >> 16),
                (byte) ((value & 0xFF00) >> 8),
                (byte) (value & 0xFF)
            };
        }

        private static byte[] Int64ToBytes(long value)
        {
            return new[]
            {
                (byte) (((ulong) value & 0xFF00000000000000) >> 56),
                (byte) ((value & 0xFF000000000000) >> 48),
                (byte) ((value & 0xFF0000000000) >> 40),
                (byte) ((value & 0xFF00000000) >> 32),
                (byte) ((value & 0xFF000000) >> 24),
                (byte) ((value & 0xFF0000) >> 16),
                (byte) ((value & 0xFF00) >> 8),
                (byte) (value & 0xFF)
            };
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

        private static byte[] Int16ToBytes(short value)
        {
            return new[] {(byte) ((value & 0xFF00) >> 8), (byte) (value & 0xFF)};
        }

        private static DateTimeOffset BytesToDateTimeOffset(byte[] buffer, int idx)
        {
            return UnixStart.AddMilliseconds(BytesToInt64(buffer, 0));
        }

        private static byte[] DateTimeOffsetToBytes(DateTimeOffset dt)
        {
            return Int64ToBytes(Convert.ToInt64(Math.Floor((dt - UnixStart).TotalMilliseconds)));
        }

        public static object CqlConvert(byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo, Type cSharpType = null)
        {
            return Decoders[typeCode](typeInfo, buffer, cSharpType);
        }

        public static Type GetDefaultTypeFromCqlType(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return DefaultTypes[typeCode](typeInfo);
        }

        public static ColumnTypeCode GetColumnTypeCodeInfo(Type type, out IColumnInfo typeInfo)
        {
            typeInfo = null;
            if (type.IsGenericType)
            {
                if (type.Name.Equals("Nullable`1"))
                {
                    return GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out typeInfo);
                }
                if (type.GetInterface("ISet`1") != null)
                {
                    IColumnInfo keyTypeInfo;
                    ColumnTypeCode keyTypeCode = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out keyTypeInfo);
                    typeInfo = new SetColumnInfo {KeyTypeCode = keyTypeCode, KeyTypeInfo = keyTypeInfo};
                    return ColumnTypeCode.Set;
                }
                if (type.GetInterface("IDictionary`2") != null)
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
                if (type.GetInterface("IEnumerable`1") != null)
                {
                    IColumnInfo valueTypeInfo;
                    ColumnTypeCode valueTypeCode = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out valueTypeInfo);
                    typeInfo = new ListColumnInfo {ValueTypeCode = valueTypeCode, ValueTypeInfo = valueTypeInfo};
                    return ColumnTypeCode.List;
                }
            }
            else
            {
                if (type.Equals(typeof (string)))
                    return ColumnTypeCode.Varchar;
                if (type.Equals(typeof (long)))
                    return ColumnTypeCode.Bigint;
                if (type.Equals(typeof (byte[])))
                    return ColumnTypeCode.Blob;
                if (type.Equals(typeof (bool)))
                    return ColumnTypeCode.Boolean;
                if (type.Equals(TypeAdapters.DecimalTypeAdapter.GetDataType()))
                    return ColumnTypeCode.Decimal;
                if (type.Equals(typeof (double)))
                    return ColumnTypeCode.Double;
                if (type.Equals(typeof (float)))
                    return ColumnTypeCode.Float;
                if (type.Equals(typeof(IPAddress)))
                    return ColumnTypeCode.Inet;
                if (type.Equals(typeof (int)))
                    return ColumnTypeCode.Int;
                if (type.Equals(typeof (DateTimeOffset)))
                    return ColumnTypeCode.Timestamp;
                if (type.Equals(typeof (DateTime)))
                    return ColumnTypeCode.Timestamp;
                if (type.Equals(typeof (Guid)))
                    return ColumnTypeCode.Uuid;
                if (type.Equals(TypeAdapters.VarIntTypeAdapter.GetDataType()))
                    return ColumnTypeCode.Varint;
            }

            throw new InvalidOperationException("Unknown type");
        }

        public static byte[] InvCqlConvert(object value)
        {
            if (value == null)
            {
                return null;
            }
            IColumnInfo typeInfo;
            ColumnTypeCode typeCode = GetColumnTypeCodeInfo(value.GetType(), out typeInfo);
            return Encoders[typeCode](typeInfo, value);
        }

        internal static void CheckArgument(Type t, object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            if (!t.IsInstanceOfType(value))
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] {t.FullName});
        }

        internal static void CheckArgument<T>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            if (!(value is T))
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] {typeof (T).FullName});
        }

        internal static void CheckArgument<T1, T2>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            if (!(value is T1 || value is T2))
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] {typeof (T1).FullName, typeof (T2).FullName});
        }

        public static object ConvertFromAscii(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return Encoding.ASCII.GetString(value);
        }

        public static Type GetDefaultTypeFromAscii(IColumnInfo typeInfo)
        {
            return typeof (string);
        }

        public static byte[] InvConvertFromAscii(IColumnInfo typeInfo, object value)
        {
            CheckArgument<string>(value);
            return Encoding.ASCII.GetBytes((string) value);
        }

        public static object ConvertFromBlob(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return value;
        }

        public static Type GetDefaultTypeFromBlob(IColumnInfo typeInfo)
        {
            return typeof (byte[]);
        }

        public static byte[] InvConvertFromBlob(IColumnInfo typeInfo, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[]) value;
        }

        public static object ConvertFromBigint(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return BytesToInt64(value, 0);
        }

        public static Type GetDefaultTypeFromBigint(IColumnInfo typeInfo)
        {
            return typeof (long);
        }

        public static byte[] InvConvertFromBigint(IColumnInfo typeInfo, object value)
        {
            CheckArgument<long>(value);
            return Int64ToBytes((long) value);
        }

        public static object ConvertFromUuid(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return new Guid(GuidShuffle(value));
        }

        public static Type GetDefaultTypeFromUuid(IColumnInfo typeInfo)
        {
            return typeof (Guid);
        }

        public static byte[] InvConvertFromUuid(IColumnInfo typeInfo, object value)
        {
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid) value).ToByteArray());
        }

        public static object ConvertFromVarint(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return TypeAdapters.VarIntTypeAdapter.ConvertFrom(buffer);
        }

        public static Type GetDefaultTypeFromVarint(IColumnInfo typeInfo)
        {
            return TypeAdapters.VarIntTypeAdapter.GetDataType();
        }

        public static byte[] InvConvertFromVarint(IColumnInfo typeInfo, object value)
        {
            byte[] ret = TypeAdapters.VarIntTypeAdapter.ConvertTo(value);
            Array.Reverse(ret);
            return ret;
        }

        public static object ConvertFromSet(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo is SetColumnInfo)
            {
                ColumnTypeCode listTypecode = (typeInfo as SetColumnInfo).KeyTypeCode;
                IColumnInfo listTypeinfo = (typeInfo as SetColumnInfo).KeyTypeInfo;
                Type valueType = GetDefaultTypeFromCqlType(listTypecode, listTypeinfo);
                int count = BytesToUInt16(value, 0);
                int idx = 2;
                Type openType = typeof (List<>);
                Type listType = openType.MakeGenericType(valueType);
                object ret = Activator.CreateInstance(listType);
                MethodInfo addM = listType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    var valBufLen = BytesToUInt16(value, idx);
                    idx += 2;
                    var valBuf = new byte[valBufLen];
                    Buffer.BlockCopy(value, idx, valBuf, 0, valBufLen);
                    idx += valBufLen;
                    addM.Invoke(ret, new[] {CqlConvert(valBuf, listTypecode, listTypeinfo)});
                }
                return ret;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static Type GetDefaultTypeFromSet(IColumnInfo typeInfo)
        {
            if (typeInfo is SetColumnInfo)
            {
                ColumnTypeCode listTypecode = (typeInfo as SetColumnInfo).KeyTypeCode;
                IColumnInfo listTypeinfo = (typeInfo as SetColumnInfo).KeyTypeInfo;
                Type valueType = GetDefaultTypeFromCqlType(listTypecode, listTypeinfo);
                Type openType = typeof (IEnumerable<>);
                Type listType = openType.MakeGenericType(valueType);
                return listType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromSet(IColumnInfo typeInfo, object value)
        {
            Type listType = GetDefaultTypeFromSet(typeInfo);
            CheckArgument(listType, value);
            ColumnTypeCode listTypecode = (typeInfo as SetColumnInfo).KeyTypeCode;
            IColumnInfo listTypeinfo = (typeInfo as SetColumnInfo).KeyTypeInfo;

            var bufs = new List<byte[]>();
            int cnt = 0;
            int bsize = 2;
            foreach (object obj in (value as IEnumerable))
            {
                byte[] buf = InvCqlConvert(obj);
                bufs.Add(buf);
                bsize += 2; //size of value
                bsize += buf.Length;
                cnt++;
            }
            var ret = new byte[bsize];

            byte[] cntbuf = Int16ToBytes((short) cnt);

            int idx = 0;
            Buffer.BlockCopy(cntbuf, 0, ret, 0, 2);
            idx += 2;
            foreach (byte[] buf in bufs)
            {
                byte[] valBufSize = Int16ToBytes((short) buf.Length);
                Buffer.BlockCopy(valBufSize, 0, ret, idx, 2);
                idx += 2;
                Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                idx += buf.Length;
            }

            return ret;
        }

        public static object ConvertFromTimestamp(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (cSharpType == null || cSharpType.Equals(typeof(object)) || cSharpType.Equals(typeof(DateTimeOffset)))
                return BytesToDateTimeOffset(value, 0);
            return BytesToDateTimeOffset(value, 0).DateTime;
        }

        public static Type GetDefaultTypeFromTimestamp(IColumnInfo typeInfo)
        {
            return typeof (DateTimeOffset);
        }

        public static byte[] InvConvertFromTimestamp(IColumnInfo typeInfo, object value)
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

        public static object ConvertFromTimeuuid(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return new Guid(GuidShuffle(value));
        }

        public static Type GetDefaultTypeFromTimeuuid(IColumnInfo typeInfo)
        {
            return typeof (Guid);
        }

        public static byte[] InvConvertFromTimeuuid(IColumnInfo typeInfo, object value)
        {
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid) value).ToByteArray());
        }

        public static object ConvertFromMap(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo is MapColumnInfo)
            {
                ColumnTypeCode keyTypecode = (typeInfo as MapColumnInfo).KeyTypeCode;
                IColumnInfo keyTypeinfo = (typeInfo as MapColumnInfo).KeyTypeInfo;
                ColumnTypeCode valueTypecode = (typeInfo as MapColumnInfo).ValueTypeCode;
                IColumnInfo valueTypeinfo = (typeInfo as MapColumnInfo).ValueTypeInfo;
                Type keyType = GetDefaultTypeFromCqlType(keyTypecode, keyTypeinfo);
                Type valueType = GetDefaultTypeFromCqlType(valueTypecode, valueTypeinfo);
                int count = BytesToUInt16(value, 0);
                int idx = 2;
                Type openType = typeof (SortedDictionary<,>);
                Type dicType = openType.MakeGenericType(keyType, valueType);
                object ret = Activator.CreateInstance(dicType);
                MethodInfo addM = dicType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    var keyBufLen = BytesToUInt16(value, idx);
                    idx += 2;
                    var keyBuf = new byte[keyBufLen];
                    Buffer.BlockCopy(value, idx, keyBuf, 0, keyBufLen);
                    idx += keyBufLen;

                    var valueBufLen = BytesToUInt16(value, idx);
                    idx += 2;
                    var valueBuf = new byte[valueBufLen];
                    Buffer.BlockCopy(value, idx, valueBuf, 0, valueBufLen);
                    idx += valueBufLen;

                    addM.Invoke(ret, new[]
                    {
                        CqlConvert(keyBuf, keyTypecode, keyTypeinfo),
                        CqlConvert(valueBuf, valueTypecode, valueTypeinfo)
                    });
                }
                return ret;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static Type GetDefaultTypeFromMap(IColumnInfo typeInfo)
        {
            if (typeInfo is MapColumnInfo)
            {
                ColumnTypeCode keyTypecode = (typeInfo as MapColumnInfo).KeyTypeCode;
                IColumnInfo keyTypeinfo = (typeInfo as MapColumnInfo).KeyTypeInfo;
                ColumnTypeCode valueTypecode = (typeInfo as MapColumnInfo).ValueTypeCode;
                IColumnInfo valueTypeinfo = (typeInfo as MapColumnInfo).ValueTypeInfo;
                Type keyType = GetDefaultTypeFromCqlType(keyTypecode, keyTypeinfo);
                Type valueType = GetDefaultTypeFromCqlType(valueTypecode, valueTypeinfo);

                Type openType = typeof (IDictionary<,>);
                Type dicType = openType.MakeGenericType(keyType, valueType);
                return dicType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromMap(IColumnInfo typeInfo, object value)
        {
            Type dicType = GetDefaultTypeFromMap(typeInfo);
            CheckArgument(dicType, value);
            ColumnTypeCode keyTypecode = (typeInfo as MapColumnInfo).KeyTypeCode;
            IColumnInfo keyTypeinfo = (typeInfo as MapColumnInfo).KeyTypeInfo;
            ColumnTypeCode valueTypecode = (typeInfo as MapColumnInfo).ValueTypeCode;
            IColumnInfo valueTypeinfo = (typeInfo as MapColumnInfo).ValueTypeInfo;
            Type keyType = GetDefaultTypeFromCqlType(keyTypecode, keyTypeinfo);
            Type valueType = GetDefaultTypeFromCqlType(valueTypecode, valueTypeinfo);

            var kbufs = new List<byte[]>();
            var vbufs = new List<byte[]>();
            int cnt = 0;
            int bsize = 2;

            PropertyInfo keyProp = dicType.GetProperty("Keys");
            PropertyInfo valueProp = dicType.GetProperty("Values");

            foreach (object obj in keyProp.GetValue(value, new object[] {}) as IEnumerable)
            {
                byte[] buf = InvCqlConvert(obj);
                kbufs.Add(buf);
                bsize += 2; //size of key
                bsize += buf.Length;
                cnt++;
            }

            foreach (object obj in valueProp.GetValue(value, new object[] {}) as IEnumerable)
            {
                byte[] buf = InvCqlConvert(obj);
                vbufs.Add(buf);
                bsize += 2; //size of value
                bsize += buf.Length;
            }

            var ret = new byte[bsize];

            byte[] cntbuf = Int16ToBytes((short) cnt); // short or ushort ? 

            int idx = 0;
            Buffer.BlockCopy(cntbuf, 0, ret, 0, 2);
            idx += 2;
            for (int i = 0; i < cnt; i++)
            {
                {
                    byte[] buf = kbufs[i];
                    byte[] keyvalBufSize = Int16ToBytes((short) buf.Length);
                    Buffer.BlockCopy(keyvalBufSize, 0, ret, idx, 2);
                    idx += 2;
                    Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                    idx += buf.Length;
                }
                {
                    byte[] buf = vbufs[i];
                    byte[] keyvalBufSize = Int16ToBytes((short) buf.Length);
                    Buffer.BlockCopy(keyvalBufSize, 0, ret, idx, 2);
                    idx += 2;
                    Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                    idx += buf.Length;
                }
            }

            return ret;
        }

        public static object ConvertFromText(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static Type GetDefaultTypeFromText(IColumnInfo typeInfo)
        {
            return typeof (string);
        }

        public static byte[] InvConvertFromText(IColumnInfo typeInfo, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string) value);
        }

        public static object ConvertFromVarchar(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static Type GetDefaultTypeFromVarchar(IColumnInfo typeInfo)
        {
            return typeof (string);
        }

        public static byte[] InvConvertFromVarchar(IColumnInfo typeInfo, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string) value);
        }

        public static object ConvertFromList(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (typeInfo is ListColumnInfo)
            {
                ColumnTypeCode listTypecode = (typeInfo as ListColumnInfo).ValueTypeCode;
                IColumnInfo listTypeinfo = (typeInfo as ListColumnInfo).ValueTypeInfo;
                Type valueType = GetDefaultTypeFromCqlType(listTypecode, listTypeinfo);
                int count = BytesToUInt16(value, 0);
                int idx = 2;
                Type openType = typeof (List<>);
                Type listType = openType.MakeGenericType(valueType);
                object ret = Activator.CreateInstance(listType);
                MethodInfo addM = listType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    var valBufLen = BytesToUInt16(value, idx);
                    idx += 2;
                    var valBuf = new byte[valBufLen];
                    Buffer.BlockCopy(value, idx, valBuf, 0, valBufLen);
                    idx += valBufLen;
                    addM.Invoke(ret, new[] {CqlConvert(valBuf, listTypecode, listTypeinfo)});
                }
                return ret;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static Type GetDefaultTypeFromList(IColumnInfo typeInfo)
        {
            if (typeInfo is ListColumnInfo)
            {
                ColumnTypeCode listTypecode = (typeInfo as ListColumnInfo).ValueTypeCode;
                IColumnInfo listTypeinfo = (typeInfo as ListColumnInfo).ValueTypeInfo;
                Type valueType = GetDefaultTypeFromCqlType(listTypecode, listTypeinfo);
                Type openType = typeof (IEnumerable<>);
                Type listType = openType.MakeGenericType(valueType);
                return listType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromList(IColumnInfo typeInfo, object value)
        {
            Type listType = GetDefaultTypeFromList(typeInfo);
            CheckArgument(listType, value);
            ColumnTypeCode listTypecode = (typeInfo as ListColumnInfo).ValueTypeCode;
            IColumnInfo listTypeinfo = (typeInfo as ListColumnInfo).ValueTypeInfo;

            var bufs = new List<byte[]>();
            int cnt = 0;
            int bsize = 2;
            foreach (object obj in (value as IEnumerable))
            {
                byte[] buf = InvCqlConvert(obj);
                bufs.Add(buf);
                bsize += 2; //size of value
                bsize += buf.Length;
                cnt++;
            }
            var ret = new byte[bsize];

            byte[] cntbuf = Int16ToBytes((short) cnt);

            int idx = 0;
            Buffer.BlockCopy(cntbuf, 0, ret, 0, 2);
            idx += 2;
            foreach (byte[] buf in bufs)
            {
                byte[] valBufSize = Int16ToBytes((short) buf.Length);
                Buffer.BlockCopy(valBufSize, 0, ret, idx, 2);
                idx += 2;
                Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                idx += buf.Length;
            }

            return ret;
        }

        public static object ConvertFromInet(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            if (value.Length == 4 || value.Length == 16)
            {
                return new IPAddress(value);
            }
            throw new DriverInternalError("Invalid lenght of Inet Addr");
        }

        public static Type GetDefaultTypeFromInet(IColumnInfo typeInfo)
        {
            return typeof(IPAddress);
        }

        public static byte[] InvConvertFromInet(IColumnInfo typeInfo, object value)
        {
            CheckArgument<IPAddress>(value);
            return (value as IPAddress).GetAddressBytes();
        }

        public static object ConvertFromCounter(IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return BytesToInt64(buffer, 0);
        }

        public static Type GetDefaultTypeFromCounter(IColumnInfo typeInfo)
        {
            return typeof (long);
        }

        public static byte[] InvConvertFromCounter(IColumnInfo typeInfo, object value)
        {
            CheckArgument<long>(value);
            return Int64ToBytes((long) value);
        }

        public static object ConvertFromDouble(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return BitConverter.ToDouble(buffer, 0);
        }

        public static Type GetDefaultTypeFromDouble(IColumnInfo typeInfo)
        {
            return typeof (double);
        }

        public static byte[] InvConvertFromDouble(IColumnInfo typeInfo, object value)
        {
            CheckArgument<double>(value);
            byte[] ret = BitConverter.GetBytes((double) value);
            Array.Reverse(ret);
            return ret;
        }

        public static object ConvertFromInt(IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return BytesToInt32(buffer, 0);
        }

        public static Type GetDefaultTypeFromInt(IColumnInfo typeInfo)
        {
            return typeof (int);
        }

        public static byte[] InvConvertFromInt(IColumnInfo typeInfo, object value)
        {
            CheckArgument<int>(value);
            return Int32ToBytes((int) value);
        }

        public static object ConvertFromFloat(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return BitConverter.ToSingle(buffer, 0);
        }

        public static Type GetDefaultTypeFromFloat(IColumnInfo typeInfo)
        {
            return typeof (float);
        }

        public static byte[] InvConvertFromFloat(IColumnInfo typeInfo, object value)
        {
            CheckArgument<float>(value);
            byte[] ret = BitConverter.GetBytes((float) value);
            Array.Reverse(ret);
            return ret;
        }

        public static object ConvertFromCustom(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            return value;
//            return Encoding.UTF8.GetString((byte[])value);
        }

        public static Type GetDefaultTypeFromCustom(IColumnInfo typeInfo)
        {
            return typeof (byte[]);
//            return typeof(string);
        }

        public static byte[] InvConvertFromCustom(IColumnInfo typeInfo, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[]) value;
//            CheckArgument<string>(value);
//            return Encoding.UTF8.GetBytes((string)value);
        }

        public static object ConvertFromBoolean(IColumnInfo typeInfo, byte[] buffer, Type cSharpType)
        {
            return buffer[0] == 1;
        }

        public static Type GetDefaultTypeFromBoolean(IColumnInfo typeInfo)
        {
            return typeof (bool);
        }

        public static byte[] InvConvertFromBoolean(IColumnInfo typeInfo, object value)
        {
            CheckArgument<bool>(value);
            var buffer = new byte[1];
            buffer[0] = ((bool) value) ? (byte) 0x01 : (byte) 0x00;
            return buffer;
        }

        public static object ConvertFromDecimal(IColumnInfo typeInfo, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            return TypeAdapters.DecimalTypeAdapter.ConvertFrom(buffer);
        }

        public static Type GetDefaultTypeFromDecimal(IColumnInfo typeInfo)
        {
            return TypeAdapters.DecimalTypeAdapter.GetDataType();
        }

        public static byte[] InvConvertFromDecimal(IColumnInfo typeInfo, object value)
        {
            byte[] ret = TypeAdapters.DecimalTypeAdapter.ConvertTo(value);
            return ret;
        }

        private delegate Type GetDefaultTypeFromCqlTypeDel(IColumnInfo typeInfo);

    }
}