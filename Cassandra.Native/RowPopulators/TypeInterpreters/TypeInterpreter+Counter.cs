using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromCounter(TableMetadata.ColumnInfo type_info, byte[] _buffer)
        {
            Array.Reverse(_buffer);
            return BitConverter.ToInt64(_buffer, 0);
        }

        public static Type GetTypeFromCounter(TableMetadata.ColumnInfo type_info)
        {
            return typeof(long);
        }

        public static byte[] InvConvertFromCounter(TableMetadata.ColumnInfo type_info, object value)
        {
            CheckArgument<long>(value);
            return ConversionHelper.ToBytesFromInt64((long)value);
        }
    }
}
