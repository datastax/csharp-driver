using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromDecimal(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return new BigDecimal(value);
        }

        public static Type GetTypeFromDecimal(TableMetadata.ColumnInfo type_info)
        {
            return typeof(BigDecimal);
        }

        public static byte[] InvConvertFromDecimal(TableMetadata.ColumnInfo type_info, object value)
        {
            checkArgument<BigDecimal>(value);
            return ((BigDecimal)value).ToByteArray();
        }
    }
}
