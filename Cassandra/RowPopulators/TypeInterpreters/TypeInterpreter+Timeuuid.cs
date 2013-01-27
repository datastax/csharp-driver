using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromTimeuuid(IColumnInfo type_info, byte[] value)
        {
            return ConversionHelper.ToGuidFromBigEndianBytes(value);
        }

        public static Type GetTypeFromTimeuuid(IColumnInfo type_info)
        {
            return typeof(Guid);
        }

        public static byte[] InvConvertFromTimeuuid(IColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return ConversionHelper.ToBigEndianBytes((Guid)value);
        }
    }
}
