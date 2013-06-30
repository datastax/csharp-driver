using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromFloat(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            var buffer = (byte[])value.Clone();
            Array.Reverse(buffer);
            return BitConverter.ToSingle(buffer, 0);
        }

        public static Type GetDefaultTypeFromFloat(IColumnInfo type_info)
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
