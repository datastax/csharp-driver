//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Numerics;
using Cassandra.Serialization;

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
            TypeSerializer.CheckArgument<BigInteger>(value);
            return ((BigInteger) value).ToByteArray();
        }
    }
}