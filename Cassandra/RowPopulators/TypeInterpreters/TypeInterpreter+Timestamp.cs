using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromTimestamp(IColumnInfo type_info, byte[] value)
        {
            return BytesToDateTimeOffset(value,0);
        }

        public static Type GetTypeFromTimestamp(IColumnInfo type_info)
        {
            return typeof(DateTimeOffset);
        }

        public static byte[] InvConvertFromTimestamp(IColumnInfo type_info, object value)
        {
            CheckArgument<DateTimeOffset>(value);
            return DateTimeOffsetToBytes((DateTimeOffset)value);
        }
    }
}
