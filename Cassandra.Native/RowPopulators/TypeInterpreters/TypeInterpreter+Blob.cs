using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromBlob(IColumnInfo type_info, byte[] value)
        {
            return value;
        }

        public static Type GetTypeFromBlob(IColumnInfo type_info)
        {
            return typeof(byte[]);
        }

        public static byte[] InvConvertFromBlob(IColumnInfo type_info, object value)
        {
            CheckArgument<byte[]>(value);
            return (byte[]) value;
        }
    }
}
