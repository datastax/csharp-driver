using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromUuid(IColumnInfo type_info, byte[] value)
        {
            return GuidTools.FromBytes(value);
        }

        public static Type GetTypeFromUuid(IColumnInfo type_info)
        {
            return typeof(Guid);
        }

        public static byte[] InvConvertFromUuid(IColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return GuidTools.ToBytes((Guid)value);
        }
    }
}
