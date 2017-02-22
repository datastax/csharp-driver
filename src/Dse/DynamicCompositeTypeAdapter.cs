//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Cassandra.Serialization;

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
            TypeSerializer.CheckArgument<byte[]>(value);
            return (byte[]) value;
        }
    }
}