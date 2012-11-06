using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal static partial class TypeInterpreter
    {
        public static object ConvertFromTimeuuid(Metadata.ColumnInfo type_info, byte[] value)
        {
            return ConversionHelper.ToGuidFromBigEndianBytes(value);
        }

        public static Type GetTypeFromTimeuuid(Metadata.ColumnInfo type_info)
        {
            return typeof(Guid);
        }

        public static byte[] InvConvertFromTimeuuid(Metadata.ColumnInfo type_info, object value)
        {
            checkArgument<Guid>(value);
            return ConversionHelper.ToBigEndianBytes((Guid)value);
        }
    }
}
