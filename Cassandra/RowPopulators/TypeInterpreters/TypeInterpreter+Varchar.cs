using System;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromVarchar(IColumnInfo type_info, byte[] value)
        {
            return Encoding.UTF8.GetString((byte[])value);
        }

        public static Type GetTypeFromVarchar(IColumnInfo type_info)
        {
            return typeof(string);
        }

        public static byte[] InvConvertFromVarchar(IColumnInfo type_info, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string)value);
        }
    }
}
