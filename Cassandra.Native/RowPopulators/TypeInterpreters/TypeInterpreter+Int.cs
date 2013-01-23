using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromInt(IColumnInfo type_info, byte[] _buffer)
        {
            return ConversionHelper.FromBytesToInt32(_buffer, 0);
        }

        public static Type GetTypeFromInt(IColumnInfo type_info)
        {
            return typeof(int);
        }

        public static byte[] InvConvertFromInt(IColumnInfo type_info, object value)
        {
            CheckArgument<int>(value);
            return ConversionHelper.ToBytesFromInt32((int)value);
        }
    }
}
