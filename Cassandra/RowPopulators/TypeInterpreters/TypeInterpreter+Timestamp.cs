using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromTimestamp(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (cSharpType == null || cSharpType.Equals(typeof(DateTimeOffset)))
                return BytesToDateTimeOffset(value, 0);
            else
                return BytesToDateTimeOffset(value, 0).DateTime;
        }

        public static Type GetDefaultTypeFromTimestamp(IColumnInfo type_info)
        {
            return typeof(DateTimeOffset);
        }

        public static byte[] InvConvertFromTimestamp(IColumnInfo type_info, object value)
        {
            CheckArgument<DateTimeOffset, DateTime>(value);
            if(value is DateTimeOffset)
                return DateTimeOffsetToBytes((DateTimeOffset)value);
            else
                return DateTimeOffsetToBytes(new DateTimeOffset((DateTime)value));
        }
    }
}
