using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromAscii(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return Encoding.ASCII.GetString((byte[])value);
        }

        public static Type GetTypeFromAscii(TableMetadata.ColumnInfo type_info)
        {
            return typeof(string);
        }

        public static byte[] InvConvertFromAscii(TableMetadata.ColumnInfo type_info, object value)
        {
            CheckArgument<string>(value);
            return Encoding.ASCII.GetBytes((string)value);
        }
    }
}
