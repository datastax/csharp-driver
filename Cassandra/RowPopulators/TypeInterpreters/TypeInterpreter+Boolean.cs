using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromBoolean(IColumnInfo type_info, byte[] _buffer, Type cSharpType)
        {
            return _buffer[0] == 1;
        }

        public static Type GetDefaultTypeFromBoolean(IColumnInfo type_info)
        {
            return typeof(bool);
        }

        public static byte[] InvConvertFromBoolean(IColumnInfo type_info, object value)
        {
            CheckArgument<bool>(value);
            var buffer = new byte[1];
            buffer[0] = ((bool)value) ? (byte)0x01 : (byte)0x00;
            return buffer;
        }
    }
}
