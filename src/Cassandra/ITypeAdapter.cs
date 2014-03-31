using System;

namespace Cassandra
{
    public interface ITypeAdapter
    {
        Type GetDataType();
        object ConvertFrom(byte[] decimalBuf);
        byte[] ConvertTo(object value);
    }
}