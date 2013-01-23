using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromDecimal(IColumnInfo type_info, byte[] value)
        {
            return new BigDecimal(value);
        }

        public static Type GetTypeFromDecimal(IColumnInfo type_info)
        {
            return typeof(BigDecimal);
        }

        public static byte[] InvConvertFromDecimal(IColumnInfo type_info, object value)
        {
            CheckArgument<BigDecimal>(value);
            return ((BigDecimal)value).ToByteArray();
        }
    }
}
