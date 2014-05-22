using System;

namespace Cassandra
{
    /// <summary>
    /// Represents a adapter to convert a Cassandra type to a CLR type
    /// </summary>
    public interface ITypeAdapter
    {
        Type GetDataType();
        object ConvertFrom(byte[] decimalBuf);
        byte[] ConvertTo(object value);
    }
}