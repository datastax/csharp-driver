using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromTimeuuid(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return ConversionHelper.ToGuidFromBigEndianBytes(value);
        }

        public static Type GetTypeFromTimeuuid(TableMetadata.ColumnInfo type_info)
        {
            return typeof(Guid);
        }

        public static byte[] InvConvertFromTimeuuid(TableMetadata.ColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return ConversionHelper.ToBigEndianBytes((Guid)value);
        }
    }
}
