using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromBlob(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return value;
        }

        public static Type GetTypeFromBlob(TableMetadata.ColumnInfo type_info)
        {
            return typeof(byte[]);
        }

        public static byte[] InvConvertFromBlob(TableMetadata.ColumnInfo type_info, object value)
        {
            checkArgument<byte[]>(value);
            return (byte[]) value;
        }
    }
}
