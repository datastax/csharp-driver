using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromInt(TableMetadata.ColumnInfo type_info, byte[] _buffer)
        {
            return ConversionHelper.FromBytesToInt32(_buffer, 0);
        }

        public static Type GetTypeFromInt(TableMetadata.ColumnInfo type_info)
        {
            return typeof(int);
        }

        public static byte[] InvConvertFromInt(TableMetadata.ColumnInfo type_info, object value)
        {
            CheckArgument<int>(value);
            return ConversionHelper.ToBytesFromInt32((int)value);
        }
    }
}
