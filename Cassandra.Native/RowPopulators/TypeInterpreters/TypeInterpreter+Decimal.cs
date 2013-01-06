using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromDecimal(Metadata.ColumnInfo type_info, byte[] value)
        {
            return new BigDecimal(value);
        }

        public static Type GetTypeFromDecimal(Metadata.ColumnInfo type_info)
        {
            return typeof(BigDecimal);
        }

        public static byte[] InvConvertFromDecimal(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<BigDecimal>(value);
            return ((BigDecimal)value).ToByteArray();
        }
    }
}
