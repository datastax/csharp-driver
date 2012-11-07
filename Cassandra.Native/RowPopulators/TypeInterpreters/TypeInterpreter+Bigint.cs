using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromBigint(Metadata.ColumnInfo type_info, byte[] value)
        {
            Array.Reverse(value);
            return BitConverter.ToInt64(value, 0);            
        }

        public static Type GetTypeFromBigint(Metadata.ColumnInfo type_info)
        {
            return typeof(long);
        }

        public static byte[] InvConvertFromBigint(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<long>(value);
            return ConversionHelper.ToBytesFromInt64((long)value);
        }
    }
}
