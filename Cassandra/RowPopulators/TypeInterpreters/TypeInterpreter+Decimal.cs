using System;

namespace Cassandra
{


   
    internal partial class TypeInterpreter
    {

        public static object ConvertFromDecimal(IColumnInfo type_info, byte[] value)
        {
            var buffer = (byte[])value.Clone();
            return TypeAdapters.DecimalTypeAdapter.ConvertFrom(buffer);
        }

        public static Type GetTypeFromDecimal(IColumnInfo type_info)
        {
            return TypeAdapters.DecimalTypeAdapter.GetDataType();
        }

        public static byte[] InvConvertFromDecimal(IColumnInfo type_info, object value)
        {
            byte[] ret = TypeAdapters.DecimalTypeAdapter.ConvertTo(value);            
            return ret;
        }
    }
}
