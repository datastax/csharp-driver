using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace Cassandra.Native
{

    internal static partial class TypeInerpreter
    {
        public static object ConvertFromList(Metadata.ColumnInfo type_info, byte[] value)
        {
            if (type_info is Metadata.ListColumnInfo)
            {
                var list_typecode = (type_info as Metadata.ListColumnInfo).value_type_code;
                var list_typeinfo = (type_info as Metadata.ListColumnInfo).value_type_info;
                var value_type = TypeInerpreter.GetTypeFromCqlType(list_typecode, list_typeinfo);
                int count = ConversionHelper.FromBytestToInt16(value, 0);
                int idx = 2;
                var openType = typeof(List<>);
                var listType = openType.MakeGenericType(value_type);
                object ret = Activator.CreateInstance(listType);
                var addM = listType.GetMethod("Add");
                for (int i = 0; i < count; i++)
                {
                    var val_buf_len = ConversionHelper.FromBytestToInt16(value,idx);
                    idx+=2;
                    byte[] val_buf = new byte[val_buf_len];
                    Buffer.BlockCopy(value, idx, val_buf, 0, val_buf_len);
                    idx += val_buf_len;
                    addM.Invoke(ret, new object[] { TypeInerpreter.CqlConvert(val_buf,list_typecode,list_typeinfo) });
                }
                return ret;
            }
            throw new InvalidOperationException();
        }

        public static Type GetTypeFromList(Metadata.ColumnInfo type_info)
        {
            if (type_info is Metadata.ListColumnInfo)
            {
                var list_typecode = (type_info as Metadata.ListColumnInfo).value_type_code;
                var list_typeinfo = (type_info as Metadata.ListColumnInfo).value_type_info;
                var value_type = TypeInerpreter.GetTypeFromCqlType(list_typecode, list_typeinfo);
                var openType = typeof(IEnumerable<>);
                var listType = openType.MakeGenericType(value_type);
                return listType;
            }
            throw new InvalidOperationException();
        }

        public static byte[] InvConvertFromList(Metadata.ColumnInfo type_info, object value)
        {
            var listType = GetTypeFromList(type_info);
            checkArgument(listType, value);
            var list_typecode = (type_info as Metadata.ListColumnInfo).value_type_code;
            var list_typeinfo = (type_info as Metadata.ListColumnInfo).value_type_info;

            List<byte[]> bufs = new List<byte[]>();
            int cnt = 0;
            int bsize = 2;
            foreach (var obj in (value as IEnumerable))
            {
                var buf = TypeInerpreter.InvCqlConvert(obj, list_typecode, list_typeinfo);
                bufs.Add(buf);
                bsize += buf.Length;
                cnt++;
            }
            var ret = new byte[bsize];

            var cntbuf = ConversionHelper.ToBytesFromInt16((short)cnt);

            int idx = 0;
            Buffer.BlockCopy(cntbuf, 0, ret, 0, 2);
            idx += 2;
            foreach (var buf in bufs)
            {
                Buffer.BlockCopy(buf, 0, ret, idx, buf.Length);
                idx += buf.Length;
            }

            return ret;
        }
    }
}
