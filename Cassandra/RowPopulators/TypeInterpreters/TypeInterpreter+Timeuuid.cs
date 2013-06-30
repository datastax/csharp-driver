using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        public static object ConvertFromTimeuuid(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            return new Guid(GuidShuffle(value));
        }

        public static Type GetDefaultTypeFromTimeuuid(IColumnInfo type_info)
        {
            return typeof(Guid);
        }

        public static byte[] InvConvertFromTimeuuid(IColumnInfo type_info, object value)
        {
            CheckArgument<Guid>(value);
            return GuidShuffle(((Guid)value).ToByteArray());
        }
    }
}
