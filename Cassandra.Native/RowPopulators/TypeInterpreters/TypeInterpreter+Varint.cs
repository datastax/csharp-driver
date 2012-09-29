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
            byte[] number = new byte[value.Length];

            Buffer.BlockCopy(value, 0, number, 0, number.Length);

            return new VarintBuffer() { BigIntegerBytes = number };
        }

        public static Type GetTypeFromVarint(Metadata.ColumnInfo type_info)
        {
            return typeof(VarintBuffer);
        }

        public static byte[] InvConvertFromVarint(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<VarintBuffer>(value);
            byte[] ret = new byte[((VarintBuffer)value).BigIntegerBytes.Length];
            Buffer.BlockCopy(((DecimalBuffer)value).BigIntegerBytes, 0, ret, 0, ret.Length);
            return ret;
        }
    }
}
