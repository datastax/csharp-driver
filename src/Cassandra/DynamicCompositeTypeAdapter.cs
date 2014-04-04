using System;

namespace Cassandra
{
    public class DynamicCompositeTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof (byte[]);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            return decimalBuf;
        }

        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<byte[]>(value);
            return (byte[]) value;
        }
    }
}