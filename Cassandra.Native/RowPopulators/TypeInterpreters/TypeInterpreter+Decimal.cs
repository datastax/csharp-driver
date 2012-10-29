using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public struct DecimalBuffer
    {
        public byte[] BigIntegerBytes;        
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
            return ((DecimalBuffer)value).BigIntegerBytes;
        }
    }
}
