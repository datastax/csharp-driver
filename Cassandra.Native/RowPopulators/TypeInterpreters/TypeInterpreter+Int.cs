using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal static partial class TypeInterpreter
    {
        public static object ConvertFromInt(Metadata.ColumnInfo type_info, byte[] _buffer)
        {
            return ConversionHelper.FromBytesToInt32(_buffer, 0);
        }

        public static Type GetTypeFromInt(Metadata.ColumnInfo type_info)
        {
            return typeof(int);
        }

        public static byte[] InvConvertFromInt(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<int>(value);
            return ConversionHelper.ToBytesFromInt32((int)value);
        }
    }
}
