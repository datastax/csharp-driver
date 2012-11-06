using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public struct VarintBuffer : IEquatable<object>
    {
        public byte[] BigIntegerBytes;
        public override bool Equals(object db)
        {
            if (this.BigIntegerBytes == null || ((VarintBuffer)db).BigIntegerBytes == null)
                return false;

            if (this.BigIntegerBytes.Length != ((VarintBuffer)db).BigIntegerBytes.Length)
                return false;

            for (int i = 0; i < this.BigIntegerBytes.Length; i++)
                if (!Object.Equals(this.BigIntegerBytes[i], ((VarintBuffer)db).BigIntegerBytes[i]))
                    return false;
            return true;
        }
    }

    internal static partial class TypeInterpreter
    {
        public static object ConvertFromVarint(Metadata.ColumnInfo type_info, byte[] value)
        {
            Array.Reverse(value);
            return new VarintBuffer() { BigIntegerBytes = value };
        }

        public static Type GetTypeFromVarint(Metadata.ColumnInfo type_info)
        {
            return typeof(VarintBuffer);
        }

        public static byte[] InvConvertFromVarint(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<VarintBuffer>(value);
            
            byte[] bigIntBytes = new byte[((VarintBuffer)value).BigIntegerBytes.Length];
            Buffer.BlockCopy(((VarintBuffer)value).BigIntegerBytes, 0, bigIntBytes, 0, bigIntBytes.Length);
            Array.Reverse(bigIntBytes);
            return bigIntBytes;
        }
    }
}
