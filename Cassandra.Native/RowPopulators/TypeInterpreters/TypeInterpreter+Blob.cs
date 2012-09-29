using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal static partial class TypeInerpreter
    {
        public static object ConvertFromBlob(Metadata.ColumnInfo type_info, byte[] value)
        {
            return value;
        }

        public static Type GetTypeFromBlob(Metadata.ColumnInfo type_info)
        {
            return typeof(byte[]);
        }

        public static byte[] InvConvertFromBlob(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<byte[]>(value);
            return (byte[]) value;
        }
    }
}
