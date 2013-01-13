using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromUuid(TableMetadata.ColumnInfo type_info, byte[] value)
        {
            return ConversionHelper.ToGuidFromBigEndianBytes(value);
        }

        public static Type GetTypeFromUuid(TableMetadata.ColumnInfo type_info)
        {
            return typeof(Guid);
        }

        public static byte[] InvConvertFromUuid(TableMetadata.ColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return ConversionHelper.ToBigEndianBytes((Guid)value);
        }
    }
}
