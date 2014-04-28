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
using System.Collections.Generic;
using System.Collections;

namespace Cassandra
{

    internal partial class TypeInterpreter
    {
        public static object ConvertFromMap(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (type_info is MapColumnInfo)
            {
                var key_typecode = (type_info as MapColumnInfo).KeyTypeCode;
                var key_typeinfo = (type_info as MapColumnInfo).KeyTypeInfo;
                var value_typecode = (type_info as MapColumnInfo).ValueTypeCode;
                var value_typeinfo = (type_info as MapColumnInfo).ValueTypeInfo;
                var key_type = TypeInterpreter.GetDefaultTypeFromCqlType(key_typecode, key_typeinfo);
                var value_type = TypeInterpreter.GetDefaultTypeFromCqlType(value_typecode, value_typeinfo);
                int count = BytesToUInt16(value, 0);
                int idx = 2;
                var openType = typeof(SortedDictionary<,>);
                var dicType = openType.MakeGenericType(key_type, value_type);
                object ret = Activator.CreateInstance(dicType);
                var addM = dicType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    var key_buf_len = BytesToUInt16(value, idx);
                    idx += 2;
                    byte[] key_buf = new byte[key_buf_len];
                    Buffer.BlockCopy(value, idx, key_buf, 0, key_buf_len);
                    idx += key_buf_len;

                    var value_buf_len = BytesToUInt16(value, idx);
                    idx += 2;
                    byte[] value_buf = new byte[value_buf_len];
                    Buffer.BlockCopy(value, idx, value_buf, 0, value_buf_len);
                    idx += value_buf_len;

                    addM.Invoke(ret, new object[] {
                        TypeInterpreter.CqlConvert(key_buf, key_typecode, key_typeinfo),
                        TypeInterpreter.CqlConvert(value_buf, value_typecode, value_typeinfo)
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
                var key_typecode = (type_info as MapColumnInfo).KeyTypeCode;
                var key_typeinfo = (type_info as MapColumnInfo).KeyTypeInfo;
                var value_typecode = (type_info as MapColumnInfo).ValueTypeCode;
                var value_typeinfo = (type_info as MapColumnInfo).ValueTypeInfo;
                var key_type = TypeInterpreter.GetDefaultTypeFromCqlType(key_typecode, key_typeinfo);
                var value_type = TypeInterpreter.GetDefaultTypeFromCqlType(value_typecode, value_typeinfo);

                var openType = typeof(IDictionary<,>);
                var dicType = openType.MakeGenericType(key_type, value_type);
                return dicType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromMap(IColumnInfo type_info, object value)
        {
            var dicType = GetDefaultTypeFromMap(type_info);
            CheckArgument(dicType, value);
            var key_typecode = (type_info as MapColumnInfo).KeyTypeCode;
            var key_typeinfo = (type_info as MapColumnInfo).KeyTypeInfo;
            var value_typecode = (type_info as MapColumnInfo).ValueTypeCode;
            var value_typeinfo = (type_info as MapColumnInfo).ValueTypeInfo;
            var key_type = TypeInterpreter.GetDefaultTypeFromCqlType(key_typecode, key_typeinfo);
            var value_type = TypeInterpreter.GetDefaultTypeFromCqlType(value_typecode, value_typeinfo);

            List<byte[]> kbufs = new List<byte[]>();
            List<byte[]> vbufs = new List<byte[]>();
            int cnt = 0;
            int bsize = 2;

            var key_prop = dicType.GetProperty("Keys");
            var value_prop = dicType.GetProperty("Values");

            foreach (var obj in key_prop.GetValue(value, new object[] { }) as IEnumerable)
            {
                var buf = TypeInterpreter.InvCqlConvert(obj, key_typecode, key_typeinfo);
                kbufs.Add(buf);
                bsize += 2; //size of key
                bsize += buf.Length;
                cnt++;
            }

            foreach (var obj in value_prop.GetValue(value, new object[] { }) as IEnumerable)
            {
                var buf = TypeInterpreter.InvCqlConvert(obj, value_typecode, value_typeinfo);
                vbufs.Add(buf);
                bsize += 2; //size of value
                bsize += buf.Length;
            }

            var ret = new byte[bsize];

            var cntbuf = Int16ToBytes((short)cnt); // short or ushort ? 

            int idx = 0;
            Buffer.BlockCopy(cntbuf, 0, ret, 0, 2);
            idx += 2;
            for (int i = 0; i < cnt; i++)
            {
                {
                    var buf = kbufs[i];
                    var keyval_buf_size = Int16ToBytes((short)buf.Length);
                    Buffer.BlockCopy(keyval_buf_size, 0, ret, idx, 2);
                    idx += 2;
                    Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                    idx += buf.Length;
                }
                {
                    var buf = vbufs[i];
                    var keyval_buf_size = Int16ToBytes((short)buf.Length);
                    Buffer.BlockCopy(keyval_buf_size, 0, ret, idx, 2);
                    idx += 2;
                    Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                    idx += buf.Length;
                }
            }

            return ret;
        }
    }
}
