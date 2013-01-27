using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromDouble(IColumnInfo type_info, byte[] _buffer)
        {
            Array.Reverse(_buffer);
            return BitConverter.ToDouble(_buffer, 0);
        }

        public static Type GetTypeFromDouble(IColumnInfo type_info)
        {
            return typeof(double);
        }

        public static byte[] InvConvertFromDouble(IColumnInfo type_info, object value)
        {
            CheckArgument<double>(value);
            byte[] ret = BitConverter.GetBytes((double)value);
            Array.Reverse(ret);
            return ret;
        }
    }
}
