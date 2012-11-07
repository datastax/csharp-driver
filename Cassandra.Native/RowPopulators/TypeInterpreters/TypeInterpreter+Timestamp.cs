using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromTimestamp(Metadata.ColumnInfo type_info, byte[] value)
        {
            Array.Reverse(value);
            return ConversionHelper.FromUnixTime(BitConverter.ToInt64(value, 0));
        }

        public static Type GetTypeFromTimestamp(Metadata.ColumnInfo type_info)
        {
            return typeof(DateTimeOffset);
        }

        public static byte[] InvConvertFromTimestamp(Metadata.ColumnInfo type_info, object value)
        {
            return ConversionHelper.ToBytesFromInt64(ConversionHelper.ToUnixTime((DateTimeOffset)value));
        }
    }
}
