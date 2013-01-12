using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromFloat(TableMetadata.ColumnInfo type_info, byte[] _buffer)
        {
            Array.Reverse(_buffer);
            return BitConverter.ToSingle(_buffer, 0);
        }

        public static Type GetTypeFromFloat(TableMetadata.ColumnInfo type_info)
        {
            return typeof(float);
        }

        public static byte[] InvConvertFromFloat(TableMetadata.ColumnInfo type_info, object value)
        {
            CheckArgument<float>(value);
            byte[] ret = BitConverter.GetBytes((float)value);
            Array.Reverse(ret);
            return ret;
        }


    }
}
