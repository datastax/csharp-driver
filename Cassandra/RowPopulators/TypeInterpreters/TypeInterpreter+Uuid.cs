using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromUuid(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return new Guid(GuidShuffle(value));
        }

        public static Type GetDefaultTypeFromUuid(IColumnInfo type_info)
        {
            return typeof(Guid);
        }

        public static byte[] InvConvertFromUuid(IColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid)value).ToByteArray());
        }
    }
}
