using System;

namespace Cassandra
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
            CheckArgument<BigDecimal>(value);
            return ((BigDecimal)value).ToByteArray();
        }
    }
}
