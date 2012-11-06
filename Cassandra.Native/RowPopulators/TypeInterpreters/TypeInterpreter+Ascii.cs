using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal static partial class TypeInterpreter
    {
        public static object ConvertFromAscii(Metadata.ColumnInfo type_info, byte[] value)
        {
            return Encoding.ASCII.GetString((byte[])value);
        }

        public static Type GetTypeFromAscii(Metadata.ColumnInfo type_info)
        {
            return typeof(string);
        }

        public static byte[] InvConvertFromAscii(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<string>(value);
            return Encoding.ASCII.GetBytes((string)value);
        }
    }
}
