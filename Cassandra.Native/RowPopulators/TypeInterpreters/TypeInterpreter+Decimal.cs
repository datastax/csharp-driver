using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public struct DecimalBuffer
    {
        public byte[] BigIntegerBytes;
        public int Scale;
    }

    internal static partial class TypeInerpreter
    {
        public static object ConvertFromDecimal(Metadata.ColumnInfo type_info, byte[] value)
        {
            byte[] number = new byte[value.Length - 4];
            byte[] flags = new byte[4];

            Buffer.BlockCopy(value, 0, number, 0, number.Length);
            Buffer.BlockCopy(value, value.Length - 4, flags, 0, 4);

            return new DecimalBuffer() { BigIntegerBytes = number, Scale = BitConverter.ToInt32(flags, 0) };
        }

        public static Type GetTypeFromDecimal(Metadata.ColumnInfo type_info)
        {
            return typeof(DecimalBuffer);
        }

        public static byte[] InvConvertFromDecimal(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<DecimalBuffer>(value);
            byte[] ret = new byte[((DecimalBuffer)value).BigIntegerBytes.Length + 4];
            Buffer.BlockCopy(((DecimalBuffer)value).BigIntegerBytes, 0, ret, 0, ret.Length - 4);
            Buffer.BlockCopy(BitConverter.GetBytes(((DecimalBuffer)value).Scale), 0, ret, ret.Length - 4, 4);
            return ret;
        }
    }
}
