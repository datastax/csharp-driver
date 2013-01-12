using System;
using System.Collections.Generic;
using System.Text;
using System.Numerics;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromVarint(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return new BigInteger(value);
        }

        public static Type GetTypeFromVarint(TableMetadata.ColumnInfo type_info)
        {
            return typeof(BigInteger);
        }

        public static byte[] InvConvertFromVarint(TableMetadata.ColumnInfo type_info, object value)
        {
            checkArgument<BigInteger>(value);

            return ((BigInteger)value).ToByteArray();
        }
    }
}
