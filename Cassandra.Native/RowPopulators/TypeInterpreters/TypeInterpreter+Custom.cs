using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromCustom(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return Encoding.UTF8.GetString((byte[])value);
        }

        public static Type GetTypeFromCustom(TableMetadata.ColumnInfo type_info)
        {
            return typeof(string);
        }

        public static byte[] InvConvertFromCustom(TableMetadata.ColumnInfo type_info, object value)
        {
            checkArgument<string>(value);
            return Encoding.UTF8.GetBytes((string)value);
        }
    }
}
