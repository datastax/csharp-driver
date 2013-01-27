using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromFloat(IColumnInfo type_info, byte[] _buffer)
        {
            Array.Reverse(_buffer);
            return BitConverter.ToSingle(_buffer, 0);
        }

        public static Type GetTypeFromFloat(IColumnInfo type_info)
        {
            return typeof(float);
        }

        public static byte[] InvConvertFromFloat(IColumnInfo type_info, object value)
        {
            CheckArgument<float>(value);
            byte[] ret = BitConverter.GetBytes((float)value);
            Array.Reverse(ret);
            return ret;
        }


    }
}
