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
﻿using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        internal static byte[] GuidShuffle(byte[] b)
        {
            return new byte[] { b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15] };
        }

        internal static int BytesToInt32(byte[] buffer, int idx)
        {
            return (int)(
                  (buffer[idx] << 24)
                | (buffer[idx + 1] << 16 & 0xFF0000)
                | (buffer[idx + 2] << 8 & 0xFF00)
                | (buffer[idx + 3] & 0xFF)
                );
        }

        static ushort BytesToUInt16(byte[] buffer, int idx)
        {
            return (ushort)((buffer[idx] << 8) | (buffer[idx + 1] & 0xFF));
        }

        static byte[] Int32ToBytes(int value)
        {
            return new byte[] { 
                (byte)((value & 0xFF000000) >> 24), 
                (byte)((value & 0xFF0000) >> 16), 
                (byte)((value & 0xFF00) >> 8), 
                (byte)(value & 0xFF) };
        }

        static byte[] Int64ToBytes(long value)
        {
            return new byte[] { 
                (byte)(((ulong)value & 0xFF00000000000000) >> 56), 
                (byte)((value & 0xFF000000000000) >> 48), 
                (byte)((value & 0xFF0000000000) >> 40), 
                (byte)((value & 0xFF00000000) >> 32), 
                (byte)((value & 0xFF000000) >> 24), 
                (byte)((value & 0xFF0000) >> 16), 
                (byte)((value & 0xFF00) >> 8), 
                (byte)(value & 0xFF) };
        }

        static long BytesToInt64(byte[] buffer, int idx)
        {
            return (long)(
                  (((ulong)buffer[idx] << 56) & 0xFF00000000000000)
                | (((ulong)buffer[idx + 1] << 48) & 0xFF000000000000)
                | (((ulong)buffer[idx + 2] << 40) & 0xFF0000000000)
                | (((ulong)buffer[idx + 3] << 32) & 0xFF00000000)
                | (((ulong)buffer[idx + 4] << 24) & 0xFF000000)
                | (((ulong)buffer[idx + 5] << 16) & 0xFF0000)
                | (((ulong)buffer[idx + 6] << 8) & 0xFF00)
                | (((ulong)buffer[idx + 7]) & 0xFF)
                );
        }

        static byte[] Int16ToBytes(short value)
        {
            return new byte[] { (byte)((value & 0xFF00) >> 8), (byte)(value & 0xFF) };
        }

        static DateTimeOffset BytesToDateTimeOffset(byte[] buffer, int idx)
        {
            return UnixStart.AddMilliseconds(BytesToInt64(buffer, 0));
        }

        static byte[] DateTimeOffsetToBytes(DateTimeOffset dt)
        {
            return Int64ToBytes(Convert.ToInt64(Math.Floor((dt - UnixStart).TotalMilliseconds)));
        }

        static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

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

        delegate object CqlConvertDel(IColumnInfo type_info, byte[] buffer, Type cSharpType);
        delegate Type GetDefaultTypeFromCqlTypeDel(IColumnInfo type_info);
        delegate byte[] InvCqlConvertDel(IColumnInfo type_info, object value);

        static readonly CqlConvertDel[] GoMethods = new CqlConvertDel[byte.MaxValue + 1];
        static readonly GetDefaultTypeFromCqlTypeDel[] TypMethods = new GetDefaultTypeFromCqlTypeDel[byte.MaxValue + 1];
        static readonly InvCqlConvertDel[] InvMethods = new InvCqlConvertDel[byte.MaxValue + 1];

        internal static void RegisterTypeInterpreter(ColumnTypeCode type_code)
        {
            {
                var mth = typeof(TypeInterpreter).GetMethod("ConvertFrom" + (type_code.ToString()), new Type[] { typeof(IColumnInfo), typeof(byte[]), typeof(Type) });
                GoMethods[(byte)type_code] = (CqlConvertDel)Delegate.CreateDelegate(typeof(CqlConvertDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("GetDefaultTypeFrom" + (type_code.ToString()), new Type[] { typeof(IColumnInfo) });
                TypMethods[(byte)type_code] = (GetDefaultTypeFromCqlTypeDel)Delegate.CreateDelegate(typeof(GetDefaultTypeFromCqlTypeDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("InvConvertFrom" + (type_code.ToString()), new Type[] { typeof(IColumnInfo), typeof(byte[]) });
                InvMethods[(byte)type_code] = (InvCqlConvertDel)Delegate.CreateDelegate(typeof(InvCqlConvertDel), mth);
            }
        }

        public static object CqlConvert(byte[] buffer, ColumnTypeCode type_code, IColumnInfo type_info, Type cSharpType = null)
        {
            return GoMethods[(byte)type_code](type_info, buffer, cSharpType);
        }

        public static Type GetDefaultTypeFromCqlType(ColumnTypeCode type_code, IColumnInfo type_info)
        {
            return TypMethods[(byte)type_code](type_info);
        }

        public static byte[] InvCqlConvert(object value, ColumnTypeCode type_code, IColumnInfo type_info)
        {
            return InvMethods[(byte)type_code](type_info, value);
        }

        static internal void CheckArgument(Type t, object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if (!t.IsInstanceOfType(value))
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] { t.FullName });
        }

        static internal void CheckArgument<T>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if (!(value is T))
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] { typeof(T).FullName });
        }

        static internal void CheckArgument<T1, T2>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if (!(value is T1 || value is T2))
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] { typeof(T1).FullName, typeof(T2).FullName });
        }
    }
}
