using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromCounter(Metadata.ColumnInfo type_info, byte[] _buffer)
        {
            Array.Reverse(_buffer);
            return BitConverter.ToInt64(_buffer, 0);
        }

        public static Type GetTypeFromCounter(Metadata.ColumnInfo type_info)
        {
            return typeof(long);
        }

        public static byte[] InvConvertFromCounter(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<long>(value);
            return ConversionHelper.ToBytesFromInt64((long)value);
        }
    }
}
