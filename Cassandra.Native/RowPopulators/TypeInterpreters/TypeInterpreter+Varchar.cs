using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal static partial class TypeInerpreter
    {
        public static object ConvertFromVarchar(Metadata.ColumnInfo type_info, byte[] value)
        {
            return Encoding.UTF8.GetString((byte[])value);
        }

        public static Type GetTypeFromVarchar(Metadata.ColumnInfo type_info)
        {
            return typeof(string);
        }

        public static byte[] InvConvertFromVarchar(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<string>(value);
            return Encoding.UTF8.GetBytes((string)value);
        }
    }
}
