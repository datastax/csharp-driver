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
            Array.Reverse(value);
            return new DecimalBuffer() { BigIntegerBytes = value };
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
