using System;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromCustom(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return value;
//            return Encoding.UTF8.GetString((byte[])value);
        }

        public static Type GetDefaultTypeFromCustom(IColumnInfo type_info)
        {
            return typeof(byte[]);
//            return typeof(string);
        }

        public static byte[] InvConvertFromCustom(IColumnInfo type_info, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[])value;
//            CheckArgument<string>(value);
//            return Encoding.UTF8.GetBytes((string)value);
        }
    }
}
