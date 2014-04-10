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
    internal class TypeInterpreter
    {
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);
        private static readonly CqlConvertDel[] GoMethods = new CqlConvertDel[byte.MaxValue + 1];
        private static readonly GetDefaultTypeFromCqlTypeDel[] TypMethods = new GetDefaultTypeFromCqlTypeDel[byte.MaxValue + 1];
        private static readonly InvCqlConvertDel[] InvMethods = new InvCqlConvertDel[byte.MaxValue + 1];
        private static readonly Dictionary<Type, byte> MapTypeToCode = new Dictionary<Type, byte>();

        static TypeInterpreter()
        {
            RegisterTypeInterpreter(ColumnTypeCode.Ascii);
            RegisterTypeInterpreter(ColumnTypeCode.Bigint);
            RegisterTypeInterpreter(ColumnTypeCode.Blob);
            RegisterTypeInterpreter(ColumnTypeCode.Boolean);
            RegisterTypeInterpreter(ColumnTypeCode.Counter);
            RegisterTypeInterpreter(ColumnTypeCode.Custom);
            RegisterTypeInterpreter(ColumnTypeCode.Double);
            RegisterTypeInterpreter(ColumnTypeCode.Float);
            RegisterTypeInterpreter(ColumnTypeCode.Int);
            RegisterTypeInterpreter(ColumnTypeCode.Text);
            RegisterTypeInterpreter(ColumnTypeCode.Timestamp);
            RegisterTypeInterpreter(ColumnTypeCode.Uuid);
            RegisterTypeInterpreter(ColumnTypeCode.Varchar);
            RegisterTypeInterpreter(ColumnTypeCode.Timeuuid);
            RegisterTypeInterpreter(ColumnTypeCode.Inet);
            RegisterTypeInterpreter(ColumnTypeCode.List);
            RegisterTypeInterpreter(ColumnTypeCode.Map);
            RegisterTypeInterpreter(ColumnTypeCode.Set);
            RegisterTypeInterpreter(ColumnTypeCode.Decimal);
            RegisterTypeInterpreter(ColumnTypeCode.Varint);
        }

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

        private static short BytesToInt16(byte[] buffer, int idx)
        {
            return (short) ((buffer[idx] << 8) | (buffer[idx + 1] & 0xFF));
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

        internal static void RegisterTypeInterpreter(ColumnTypeCode type_code)
        {
            {
                MethodInfo mth = typeof (TypeInterpreter).GetMethod("ConvertFrom" + (type_code),
                                                                    new[] {typeof (IColumnInfo), typeof (byte[]), typeof (Type)});
                GoMethods[(byte) type_code] = (CqlConvertDel) Delegate.CreateDelegate(typeof (CqlConvertDel), mth);
            }
            {
                MethodInfo mth = typeof (TypeInterpreter).GetMethod("GetDefaultTypeFrom" + (type_code), new[] {typeof (IColumnInfo)});
                TypMethods[(byte) type_code] = (GetDefaultTypeFromCqlTypeDel) Delegate.CreateDelegate(typeof (GetDefaultTypeFromCqlTypeDel), mth);
            }
            {
                MethodInfo mth = typeof (TypeInterpreter).GetMethod("InvConvertFrom" + (type_code), new[] {typeof (IColumnInfo), typeof (byte[])});
                InvMethods[(byte) type_code] = (InvCqlConvertDel) Delegate.CreateDelegate(typeof (InvCqlConvertDel), mth);
            }
        }

        public static object CqlConvert(byte[] buffer, ColumnTypeCode type_code, IColumnInfo type_info, Type cSharpType = null)
        {
            return GoMethods[(byte) type_code](type_info, buffer, cSharpType);
        }

        public static Type GetDefaultTypeFromCqlType(ColumnTypeCode type_code, IColumnInfo type_info)
        {
            return TypMethods[(byte) type_code](type_info);
        }

        public static ColumnTypeCode GetColumnTypeCodeInfo(Type type, out IColumnInfo type_info)
        {
            type_info = null;
            if (type.IsGenericType)
            {
                if (type.Name.Equals("Nullable`1"))
                {
                    return GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out type_info);
                }
                if (type.GetInterface("ISet`1") != null)
                {
                    IColumnInfo key_type_info;
                    ColumnTypeCode key_type_code = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out key_type_info);
                    type_info = new SetColumnInfo {KeyTypeCode = key_type_code, KeyTypeInfo = key_type_info};
                    return ColumnTypeCode.Set;
                }
                if (type.GetInterface("IDictionary`2") != null)
                {
                    IColumnInfo key_type_info;
                    ColumnTypeCode key_type_code = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out key_type_info);
                    IColumnInfo value_type_info;
                    ColumnTypeCode value_type_code = GetColumnTypeCodeInfo(type.GetGenericArguments()[1], out value_type_info);
                    type_info = new MapColumnInfo
                    {
                        KeyTypeCode = key_type_code,
                        KeyTypeInfo = key_type_info,
                        ValueTypeCode = value_type_code,
                        ValueTypeInfo = value_type_info
                    };
                    return ColumnTypeCode.Map;
                }
                if (type.GetInterface("IEnumerable`1") != null)
                {
                    IColumnInfo value_type_info;
                    ColumnTypeCode value_type_code = GetColumnTypeCodeInfo(type.GetGenericArguments()[0], out value_type_info);
                    type_info = new ListColumnInfo {ValueTypeCode = value_type_code, ValueTypeInfo = value_type_info};
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
                if (type.Equals(typeof (IPEndPoint)))
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
                return null;
            IColumnInfo type_info;
            ColumnTypeCode type_code = GetColumnTypeCodeInfo(value.GetType(), out type_info);
            return InvMethods[(byte)type_code](type_info, value);
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

        public static object ConvertFromAscii(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return Encoding.ASCII.GetString(value);
        }

        public static Type GetDefaultTypeFromAscii(IColumnInfo type_info)
        {
            return typeof (string);
        }

        public static byte[] InvConvertFromAscii(IColumnInfo type_info, object value)
        {
            CheckArgument<string>(value);
            return Encoding.ASCII.GetBytes((string) value);
        }

        public static object ConvertFromBlob(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return value;
        }

        public static Type GetDefaultTypeFromBlob(IColumnInfo type_info)
        {
            return typeof (byte[]);
        }

        public static byte[] InvConvertFromBlob(IColumnInfo type_info, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[]) value;
        }

        public static object ConvertFromBigint(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return BytesToInt64(value, 0);
        }

        public static Type GetDefaultTypeFromBigint(IColumnInfo type_info)
        {
            return typeof (long);
        }

        public static byte[] InvConvertFromBigint(IColumnInfo type_info, object value)
        {
            CheckArgument<long>(value);
            return Int64ToBytes((long) value);
        }

        public static object ConvertFromUuid(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return new Guid(GuidShuffle(value));
        }

        public static Type GetDefaultTypeFromUuid(IColumnInfo type_info)
        {
            return typeof (Guid);
        }

        public static byte[] InvConvertFromUuid(IColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid) value).ToByteArray());
        }

        public static object ConvertFromVarint(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return TypeAdapters.VarIntTypeAdapter.ConvertFrom(buffer);
        }

        public static Type GetDefaultTypeFromVarint(IColumnInfo type_info)
        {
            return TypeAdapters.VarIntTypeAdapter.GetDataType();
        }

        public static byte[] InvConvertFromVarint(IColumnInfo type_info, object value)
        {
            byte[] ret = TypeAdapters.VarIntTypeAdapter.ConvertTo(value);
            Array.Reverse(ret);
            return ret;
        }

        public static object ConvertFromSet(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (type_info is SetColumnInfo)
            {
                ColumnTypeCode list_typecode = (type_info as SetColumnInfo).KeyTypeCode;
                IColumnInfo list_typeinfo = (type_info as SetColumnInfo).KeyTypeInfo;
                Type value_type = GetDefaultTypeFromCqlType(list_typecode, list_typeinfo);
                int count = BytesToInt16(value, 0);
                int idx = 2;
                Type openType = typeof (List<>);
                Type listType = openType.MakeGenericType(value_type);
                object ret = Activator.CreateInstance(listType);
                MethodInfo addM = listType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    short val_buf_len = BytesToInt16(value, idx);
                    idx += 2;
                    var val_buf = new byte[val_buf_len];
                    Buffer.BlockCopy(value, idx, val_buf, 0, val_buf_len);
                    idx += val_buf_len;
                    addM.Invoke(ret, new[] {CqlConvert(val_buf, list_typecode, list_typeinfo)});
                }
                return ret;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static Type GetDefaultTypeFromSet(IColumnInfo type_info)
        {
            if (type_info is SetColumnInfo)
            {
                ColumnTypeCode list_typecode = (type_info as SetColumnInfo).KeyTypeCode;
                IColumnInfo list_typeinfo = (type_info as SetColumnInfo).KeyTypeInfo;
                Type value_type = GetDefaultTypeFromCqlType(list_typecode, list_typeinfo);
                Type openType = typeof (IEnumerable<>);
                Type listType = openType.MakeGenericType(value_type);
                return listType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromSet(IColumnInfo type_info, object value)
        {
            Type listType = GetDefaultTypeFromSet(type_info);
            CheckArgument(listType, value);
            ColumnTypeCode list_typecode = (type_info as SetColumnInfo).KeyTypeCode;
            IColumnInfo list_typeinfo = (type_info as SetColumnInfo).KeyTypeInfo;

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
                byte[] val_buf_size = Int16ToBytes((short) buf.Length);
                Buffer.BlockCopy(val_buf_size, 0, ret, idx, 2);
                idx += 2;
                Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                idx += buf.Length;
            }

            return ret;
        }

        public static object ConvertFromTimestamp(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (cSharpType == null || cSharpType.Equals(typeof (DateTimeOffset)))
                return BytesToDateTimeOffset(value, 0);
            return BytesToDateTimeOffset(value, 0).DateTime;
        }

        public static Type GetDefaultTypeFromTimestamp(IColumnInfo type_info)
        {
            return typeof (DateTimeOffset);
        }

        public static byte[] InvConvertFromTimestamp(IColumnInfo type_info, object value)
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

        public static object ConvertFromTimeuuid(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return new Guid(GuidShuffle(value));
        }

        public static Type GetDefaultTypeFromTimeuuid(IColumnInfo type_info)
        {
            return typeof (Guid);
        }

        public static byte[] InvConvertFromTimeuuid(IColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid) value).ToByteArray());
        }

        public static object ConvertFromMap(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (type_info is MapColumnInfo)
            {
                ColumnTypeCode key_typecode = (type_info as MapColumnInfo).KeyTypeCode;
                IColumnInfo key_typeinfo = (type_info as MapColumnInfo).KeyTypeInfo;
                ColumnTypeCode value_typecode = (type_info as MapColumnInfo).ValueTypeCode;
                IColumnInfo value_typeinfo = (type_info as MapColumnInfo).ValueTypeInfo;
                Type key_type = GetDefaultTypeFromCqlType(key_typecode, key_typeinfo);
                Type value_type = GetDefaultTypeFromCqlType(value_typecode, value_typeinfo);
                int count = BytesToInt16(value, 0);
                int idx = 2;
                Type openType = typeof (SortedDictionary<,>);
                Type dicType = openType.MakeGenericType(key_type, value_type);
                object ret = Activator.CreateInstance(dicType);
                MethodInfo addM = dicType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    short key_buf_len = BytesToInt16(value, idx);
                    idx += 2;
                    var key_buf = new byte[key_buf_len];
                    Buffer.BlockCopy(value, idx, key_buf, 0, key_buf_len);
                    idx += key_buf_len;

                    short value_buf_len = BytesToInt16(value, idx);
                    idx += 2;
                    var value_buf = new byte[value_buf_len];
                    Buffer.BlockCopy(value, idx, value_buf, 0, value_buf_len);
                    idx += value_buf_len;

                    addM.Invoke(ret, new[]
                    {
                        CqlConvert(key_buf, key_typecode, key_typeinfo),
                        CqlConvert(value_buf, value_typecode, value_typeinfo)
                    });
                }
                return ret;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static Type GetDefaultTypeFromMap(IColumnInfo type_info)
        {
            if (type_info is MapColumnInfo)
            {
                ColumnTypeCode key_typecode = (type_info as MapColumnInfo).KeyTypeCode;
                IColumnInfo key_typeinfo = (type_info as MapColumnInfo).KeyTypeInfo;
                ColumnTypeCode value_typecode = (type_info as MapColumnInfo).ValueTypeCode;
                IColumnInfo value_typeinfo = (type_info as MapColumnInfo).ValueTypeInfo;
                Type key_type = GetDefaultTypeFromCqlType(key_typecode, key_typeinfo);
                Type value_type = GetDefaultTypeFromCqlType(value_typecode, value_typeinfo);

                Type openType = typeof (IDictionary<,>);
                Type dicType = openType.MakeGenericType(key_type, value_type);
                return dicType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromMap(IColumnInfo type_info, object value)
        {
            Type dicType = GetDefaultTypeFromMap(type_info);
            CheckArgument(dicType, value);
            ColumnTypeCode key_typecode = (type_info as MapColumnInfo).KeyTypeCode;
            IColumnInfo key_typeinfo = (type_info as MapColumnInfo).KeyTypeInfo;
            ColumnTypeCode value_typecode = (type_info as MapColumnInfo).ValueTypeCode;
            IColumnInfo value_typeinfo = (type_info as MapColumnInfo).ValueTypeInfo;
            Type key_type = GetDefaultTypeFromCqlType(key_typecode, key_typeinfo);
            Type value_type = GetDefaultTypeFromCqlType(value_typecode, value_typeinfo);

            var kbufs = new List<byte[]>();
            var vbufs = new List<byte[]>();
            int cnt = 0;
            int bsize = 2;

            PropertyInfo key_prop = dicType.GetProperty("Keys");
            PropertyInfo value_prop = dicType.GetProperty("Values");

            foreach (object obj in key_prop.GetValue(value, new object[] {}) as IEnumerable)
            {
                byte[] buf = InvCqlConvert(obj);
                kbufs.Add(buf);
                bsize += 2; //size of key
                bsize += buf.Length;
                cnt++;
            }

            foreach (object obj in value_prop.GetValue(value, new object[] {}) as IEnumerable)
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
                    byte[] keyval_buf_size = Int16ToBytes((short) buf.Length);
                    Buffer.BlockCopy(keyval_buf_size, 0, ret, idx, 2);
                    idx += 2;
                    Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                    idx += buf.Length;
                }
                {
                    byte[] buf = vbufs[i];
                    byte[] keyval_buf_size = Int16ToBytes((short) buf.Length);
                    Buffer.BlockCopy(keyval_buf_size, 0, ret, idx, 2);
                    idx += 2;
                    Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                    idx += buf.Length;
                }
            }

            return ret;
        }

        public static object ConvertFromText(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static Type GetDefaultTypeFromText(IColumnInfo type_info)
        {
            return typeof (string);
        }

        public static byte[] InvConvertFromText(IColumnInfo type_info, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string) value);
        }

        public static object ConvertFromVarchar(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static Type GetDefaultTypeFromVarchar(IColumnInfo type_info)
        {
            return typeof (string);
        }

        public static byte[] InvConvertFromVarchar(IColumnInfo type_info, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string) value);
        }

        public static object ConvertFromList(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (type_info is ListColumnInfo)
            {
                ColumnTypeCode list_typecode = (type_info as ListColumnInfo).ValueTypeCode;
                IColumnInfo list_typeinfo = (type_info as ListColumnInfo).ValueTypeInfo;
                Type value_type = GetDefaultTypeFromCqlType(list_typecode, list_typeinfo);
                int count = BytesToInt16(value, 0);
                int idx = 2;
                Type openType = typeof (List<>);
                Type listType = openType.MakeGenericType(value_type);
                object ret = Activator.CreateInstance(listType);
                MethodInfo addM = listType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    short val_buf_len = BytesToInt16(value, idx);
                    idx += 2;
                    var val_buf = new byte[val_buf_len];
                    Buffer.BlockCopy(value, idx, val_buf, 0, val_buf_len);
                    idx += val_buf_len;
                    addM.Invoke(ret, new[] {CqlConvert(val_buf, list_typecode, list_typeinfo)});
                }
                return ret;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static Type GetDefaultTypeFromList(IColumnInfo type_info)
        {
            if (type_info is ListColumnInfo)
            {
                ColumnTypeCode list_typecode = (type_info as ListColumnInfo).ValueTypeCode;
                IColumnInfo list_typeinfo = (type_info as ListColumnInfo).ValueTypeInfo;
                Type value_type = GetDefaultTypeFromCqlType(list_typecode, list_typeinfo);
                Type openType = typeof (IEnumerable<>);
                Type listType = openType.MakeGenericType(value_type);
                return listType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromList(IColumnInfo type_info, object value)
        {
            Type listType = GetDefaultTypeFromList(type_info);
            CheckArgument(listType, value);
            ColumnTypeCode list_typecode = (type_info as ListColumnInfo).ValueTypeCode;
            IColumnInfo list_typeinfo = (type_info as ListColumnInfo).ValueTypeInfo;

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
                byte[] val_buf_size = Int16ToBytes((short) buf.Length);
                Buffer.BlockCopy(val_buf_size, 0, ret, idx, 2);
                idx += 2;
                Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                idx += buf.Length;
            }

            return ret;
        }

        public static object ConvertFromInet(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (value.Length == 4 || value.Length == 16)
            {
                var ip = new IPAddress(value);
                return new IPEndPoint(ip, 0);
            }
            else
            {
                byte length = value[0];
                IPAddress ip;
                int port;
                var buf = new byte[length];
                if (length == 4)
                {
                    Buffer.BlockCopy(value, 1, buf, 0, 4);
                    ip = new IPAddress(buf);
                    port = BytesToInt32(buf, 1 + 4);
                    return new IPEndPoint(ip, port);
                }
                if (length == 16)
                {
                    Buffer.BlockCopy(value, 1, buf, 0, 16);
                    ip = new IPAddress(buf);
                    port = BytesToInt32(buf, 1 + 16);
                    return new IPEndPoint(ip, port);
                }
            }
            throw new DriverInternalError("Invalid lenght of Inet Addr");
        }

        public static Type GetDefaultTypeFromInet(IColumnInfo type_info)
        {
            return typeof (IPEndPoint);
        }

        public static byte[] InvConvertFromInet(IColumnInfo type_info, object value)
        {
            CheckArgument<IPEndPoint>(value);
            byte[] addrbytes = (value as IPEndPoint).Address.GetAddressBytes();
            byte[] port = Int32ToBytes((value as IPEndPoint).Port);
            var ret = new byte[addrbytes.Length + 4 + 1];
            ret[0] = (byte) addrbytes.Length;
            Buffer.BlockCopy(addrbytes, 0, ret, 1, addrbytes.Length);
            Buffer.BlockCopy(port, 0, ret, 1 + addrbytes.Length, port.Length);
            return ret;
        }

        public static object ConvertFromCounter(IColumnInfo type_info, byte[] _buffer, Type cSharpType)
        {
            return BytesToInt64(_buffer, 0);
        }

        public static Type GetDefaultTypeFromCounter(IColumnInfo type_info)
        {
            return typeof (long);
        }

        public static byte[] InvConvertFromCounter(IColumnInfo type_info, object value)
        {
            CheckArgument<long>(value);
            return Int64ToBytes((long) value);
        }

        public static object ConvertFromDouble(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return BitConverter.ToDouble(buffer, 0);
        }

        public static Type GetDefaultTypeFromDouble(IColumnInfo type_info)
        {
            return typeof (double);
        }

        public static byte[] InvConvertFromDouble(IColumnInfo type_info, object value)
        {
            CheckArgument<double>(value);
            byte[] ret = BitConverter.GetBytes((double) value);
            Array.Reverse(ret);
            return ret;
        }

        public static object ConvertFromInt(IColumnInfo type_info, byte[] _buffer, Type cSharpType)
        {
            return BytesToInt32(_buffer, 0);
        }

        public static Type GetDefaultTypeFromInt(IColumnInfo type_info)
        {
            return typeof (int);
        }

        public static byte[] InvConvertFromInt(IColumnInfo type_info, object value)
        {
            CheckArgument<int>(value);
            return Int32ToBytes((int) value);
        }

        public static object ConvertFromFloat(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            Array.Reverse(buffer);
            return BitConverter.ToSingle(buffer, 0);
        }

        public static Type GetDefaultTypeFromFloat(IColumnInfo type_info)
        {
            return typeof (float);
        }

        public static byte[] InvConvertFromFloat(IColumnInfo type_info, object value)
        {
            CheckArgument<float>(value);
            byte[] ret = BitConverter.GetBytes((float) value);
            Array.Reverse(ret);
            return ret;
        }

        public static object ConvertFromCustom(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return value;
//            return Encoding.UTF8.GetString((byte[])value);
        }

        public static Type GetDefaultTypeFromCustom(IColumnInfo type_info)
        {
            return typeof (byte[]);
//            return typeof(string);
        }

        public static byte[] InvConvertFromCustom(IColumnInfo type_info, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[]) value;
//            CheckArgument<string>(value);
//            return Encoding.UTF8.GetBytes((string)value);
        }

        public static object ConvertFromBoolean(IColumnInfo type_info, byte[] _buffer, Type cSharpType)
        {
            return _buffer[0] == 1;
        }

        public static Type GetDefaultTypeFromBoolean(IColumnInfo type_info)
        {
            return typeof (bool);
        }

        public static byte[] InvConvertFromBoolean(IColumnInfo type_info, object value)
        {
            CheckArgument<bool>(value);
            var buffer = new byte[1];
            buffer[0] = ((bool) value) ? (byte) 0x01 : (byte) 0x00;
            return buffer;
        }

        public static object ConvertFromDecimal(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            var buffer = (byte[]) value.Clone();
            return TypeAdapters.DecimalTypeAdapter.ConvertFrom(buffer);
        }

        public static Type GetDefaultTypeFromDecimal(IColumnInfo type_info)
        {
            return TypeAdapters.DecimalTypeAdapter.GetDataType();
        }

        public static byte[] InvConvertFromDecimal(IColumnInfo type_info, object value)
        {
            byte[] ret = TypeAdapters.DecimalTypeAdapter.ConvertTo(value);
            return ret;
        }

        private delegate object CqlConvertDel(IColumnInfo type_info, byte[] buffer, Type cSharpType);

        private delegate Type GetDefaultTypeFromCqlTypeDel(IColumnInfo type_info);

        private delegate byte[] InvCqlConvertDel(IColumnInfo type_info, object value);
    }
}