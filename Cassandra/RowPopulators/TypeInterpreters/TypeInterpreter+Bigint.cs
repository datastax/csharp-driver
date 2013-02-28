using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromBigint(IColumnInfo type_info, byte[] value)
        {
            return BytesToInt64(value, 0);
        }

        public static Type GetTypeFromBigint(IColumnInfo type_info)
        {
            return typeof(long);
        }

        public static byte[] InvConvertFromBigint(IColumnInfo type_info, object value)
        {
            CheckArgument<long>(value);
            return Int64ToBytes((long)value);
        }
    }
}
