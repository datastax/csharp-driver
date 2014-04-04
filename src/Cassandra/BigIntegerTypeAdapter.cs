using System;
using System.Numerics;

namespace Cassandra
{
    public class BigIntegerTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof (BigInteger);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            return new BigInteger(decimalBuf);
        }

        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<BigInteger>(value);
            return ((BigInteger) value).ToByteArray();
        }
    }
}