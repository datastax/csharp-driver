using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromDecimal(IColumnInfo type_info, byte[] value)
        {
            Array.Reverse(value);
            return new BigDecimal(value);
        }

        public static Type GetTypeFromDecimal(IColumnInfo type_info)
        {
            return typeof(BigDecimal);
        }

        public static byte[] InvConvertFromDecimal(IColumnInfo type_info, object value)
        {
            CheckArgument<BigDecimal, decimal>(value);
            byte[] ret = null;
            if (value.GetType() == typeof(BigDecimal))
                ret = ((BigDecimal)value).ToByteArray();
            else
                if (value.GetType() == typeof(decimal))
                    ret = (new BigDecimal((decimal)value)).ToByteArray();                                        
            
            Array.Reverse(ret);
            return ret;
        }
    }
}
