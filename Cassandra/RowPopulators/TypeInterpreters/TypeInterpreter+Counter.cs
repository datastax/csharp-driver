using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromCounter(IColumnInfo type_info, byte[] _buffer)
        {
            return BytesToInt64(_buffer, 0);
        }

        public static Type GetTypeFromCounter(IColumnInfo type_info)
        {
            return typeof(long);
        }

        public static byte[] InvConvertFromCounter(IColumnInfo type_info, object value)
        {
            CheckArgument<long>(value);
            return Int64ToBytes((long)value);
        }
    }
}
