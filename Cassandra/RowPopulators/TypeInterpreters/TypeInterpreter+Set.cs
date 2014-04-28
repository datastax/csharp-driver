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
        public static object ConvertFromSet(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (type_info is SetColumnInfo)
            {
                var list_typecode = (type_info as SetColumnInfo).KeyTypeCode;
                var list_typeinfo = (type_info as SetColumnInfo).KeyTypeInfo;
                var value_type = TypeInterpreter.GetDefaultTypeFromCqlType(list_typecode, list_typeinfo);
                int count = BytesToUInt16(value, 0);
                int idx = 2;
                var openType = typeof(List<>);
                var listType = openType.MakeGenericType(value_type);
                object ret = Activator.CreateInstance(listType);
                var addM = listType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    var val_buf_len = BytesToUInt16(value, idx);
                    idx += 2;
                    byte[] val_buf = new byte[val_buf_len];
                    Buffer.BlockCopy(value, idx, val_buf, 0, val_buf_len);
                    idx += val_buf_len;
                    addM.Invoke(ret, new object[] { TypeInterpreter.CqlConvert(val_buf, list_typecode, list_typeinfo) });
                }
                return ret;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static Type GetDefaultTypeFromSet(IColumnInfo type_info)
        {
            if (type_info is SetColumnInfo)
            {
                var list_typecode = (type_info as SetColumnInfo).KeyTypeCode;
                var list_typeinfo = (type_info as SetColumnInfo).KeyTypeInfo;
                var value_type = TypeInterpreter.GetDefaultTypeFromCqlType(list_typecode, list_typeinfo);
                var openType = typeof(IEnumerable<>);
                var listType = openType.MakeGenericType(value_type);
                return listType;
            }
            throw new DriverInternalError("Invalid ColumnInfo");
        }

        public static byte[] InvConvertFromSet(IColumnInfo type_info, object value)
        {
            var listType = GetDefaultTypeFromSet(type_info);
            CheckArgument(listType, value);
            var list_typecode = (type_info as SetColumnInfo).KeyTypeCode;
            var list_typeinfo = (type_info as SetColumnInfo).KeyTypeInfo;

            List<byte[]> bufs = new List<byte[]>();
            int cnt = 0;
            int bsize = 2;
            foreach (var obj in (value as IEnumerable))
            {
                var buf = TypeInterpreter.InvCqlConvert(obj, list_typecode, list_typeinfo);
                bufs.Add(buf);
                bsize += 2; //size of value
                bsize += buf.Length;
                cnt++;
            }
            var ret = new byte[bsize];

            var cntbuf = Int16ToBytes((short)cnt);

            int idx = 0;
            Buffer.BlockCopy(cntbuf, 0, ret, 0, 2);
            idx += 2;
            foreach (var buf in bufs)
            {
                var val_buf_size = Int16ToBytes((short)buf.Length);
                Buffer.BlockCopy(val_buf_size, 0, ret, idx, 2);
                idx += 2;
                Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                idx += buf.Length;
            }

            return ret;
        }
    }
}
