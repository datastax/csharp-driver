using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal static partial class TypeInterpreter
    {
        public static object ConvertFromDouble(Metadata.ColumnInfo type_info, byte[] _buffer)
        {
            Array.Reverse(_buffer);
            return BitConverter.ToDouble(_buffer, 0);
        }

        public static Type GetTypeFromDouble(Metadata.ColumnInfo type_info)
        {
            return typeof(double);
        }

        public static byte[] InvConvertFromDouble(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<double>(value);
            byte[] ret = BitConverter.GetBytes((double)value);
            Array.Reverse(ret);
            return ret;
        }
    }
}
