//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System;
using Cassandra;

namespace BigDecimalSamples
{
    public class IKVMDecimalTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(java.math.BigDecimal);
        }

        public byte[] ConvertTo(object value)
        {
            java.math.BigInteger bi = ((java.math.BigDecimal)value).unscaledValue();
            int scale = ((java.math.BigDecimal)value).scale();
            byte[] bibytes = bi.toByteArray();
            byte[] sbytes = BitConverter.GetBytes(scale);
            Array.Reverse(sbytes);
            byte[] bytes = new byte[bibytes.Length + 4];

            for (int i = 0; i < 4; i++)
                bytes[i] = sbytes[i];

            for (int i = 4; i < bibytes.Length + 4; i++)
                bytes[i] = bibytes[i - 4];

            return bytes;
        }

        public object ConvertFrom(byte[] bytes)
        {
            byte[] scaleB = new byte[4];
            for (int i = 0; i < 4; i++)
                scaleB[3 - i] = bytes[i];

            int scale = BitConverter.ToInt32(scaleB, 0);

            byte[] bibytes = new byte[bytes.Length - 4];
            for (int i = 0; i < bibytes.Length; i++)
                bibytes[i] = bytes[i + 4];

            java.math.BigInteger bi = new java.math.BigInteger(bibytes);
            return new java.math.BigDecimal(bi, scale);
        }
    }
}
