using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public struct VarintBuffer
    {
        public byte[] BigIntegerBytes;
    }

    internal static partial class TypeInerpreter
    {
        public static object ConvertFromVarint(Metadata.ColumnInfo type_info, byte[] value)
        {
            return new VarintBuffer() { BigIntegerBytes = value };
        }

        public static Type GetTypeFromVarint(Metadata.ColumnInfo type_info)
        {
            return typeof(VarintBuffer);
        }

        public static byte[] InvConvertFromVarint(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<VarintBuffer>(value);
            return ((VarintBuffer)value).BigIntegerBytes;
        }
    }
}
