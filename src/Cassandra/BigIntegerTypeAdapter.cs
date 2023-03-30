//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
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