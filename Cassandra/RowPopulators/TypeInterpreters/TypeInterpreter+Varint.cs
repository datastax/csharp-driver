using System;
using System.Numerics;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromVarint(IColumnInfo type_info, byte[] value)
        {
            var buffer = (byte[])value.Clone();
            Array.Reverse(buffer);
            return TypeAdapters.VarIntTypeAdapter.ConvertFrom(buffer);
        }

        public static Type GetTypeFromVarint(IColumnInfo type_info)
        {
            return TypeAdapters.VarIntTypeAdapter.GetDataType();
        }

        public static byte[] InvConvertFromVarint(IColumnInfo type_info, object value)
        {
            var ret = TypeAdapters.VarIntTypeAdapter.ConvertTo(value);
            Array.Reverse(ret);
            return ret;
        }
    }
}
