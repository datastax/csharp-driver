using System;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromAscii(IColumnInfo type_info, byte[] value)
        {
            return Encoding.ASCII.GetString((byte[])value);
        }

        public static Type GetTypeFromAscii(IColumnInfo type_info)
        {
            return typeof(string);
        }

        public static byte[] InvConvertFromAscii(IColumnInfo type_info, object value)
        {
            CheckArgument<string>(value);
            return Encoding.ASCII.GetBytes((string)value);
        }
    }
}
