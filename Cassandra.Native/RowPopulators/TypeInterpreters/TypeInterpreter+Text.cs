using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromText(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return Encoding.UTF8.GetString((byte[])value);
        }

        public static Type GetTypeFromText(TableMetadata.ColumnInfo type_info)
        {
            return typeof(string);
        }

        public static byte[] InvConvertFromText(TableMetadata.ColumnInfo type_info, object value)
        {
            CheckArgument<string>(value);
            return Encoding.UTF8.GetBytes((string)value);
        }
    }
}
