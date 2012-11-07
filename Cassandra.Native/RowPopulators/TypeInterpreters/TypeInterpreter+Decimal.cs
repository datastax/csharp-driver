using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public struct DecimalBuffer : IEquatable<object>
    {
        public byte[] BigIntegerBytes;
        public override bool Equals(object db)
        {
            if (this.BigIntegerBytes == null || ((DecimalBuffer)db).BigIntegerBytes == null)
                return false;

            if (this.BigIntegerBytes.Length != ((DecimalBuffer)db).BigIntegerBytes.Length)
                return false;

            for (int i = 0; i < this.BigIntegerBytes.Length; i++)
                if (!Object.Equals(this.BigIntegerBytes[i], ((DecimalBuffer)db).BigIntegerBytes[i]))
                    return false;
            return true;
        }
    }

    internal partial class TypeInterpreter
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

            byte[] bigIntBytes = new byte[((DecimalBuffer)value).BigIntegerBytes.Length];
            Buffer.BlockCopy(((DecimalBuffer)value).BigIntegerBytes, 0, bigIntBytes, 0, bigIntBytes.Length);
            Array.Reverse(bigIntBytes); // Cassandra expects reversed array(why?)
            return bigIntBytes;
        }
    }
}
