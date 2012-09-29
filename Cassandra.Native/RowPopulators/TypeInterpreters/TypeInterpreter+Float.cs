using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal static partial class TypeInerpreter
    {
        public static object ConvertFromFloat(Metadata.ColumnInfo type_info, byte[] _buffer)
        {
            Array.Reverse(_buffer);
            return BitConverter.ToSingle(_buffer, 0);
        }

        public static Type GetTypeFromFloat(Metadata.ColumnInfo type_info)
        {
            return typeof(float);
        }

        public static byte[] InvConvertFromFloat(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<float>(value);
            return BitConverter.GetBytes((float)value);
        }
    }
}
