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
using System.Collections.Generic;
using System.Text;
using System.Numerics;
using System.IO;

namespace Cassandra
{
    public interface ITypeAdapter
    {
        Type GetDataType();
        object ConvertFrom(byte[] decimalBuf);
        byte[] ConvertTo(object value);
    }

    public static class TypeAdapters
    {
        public static ITypeAdapter DecimalTypeAdapter = new DecimalTypeAdapter();
        public static ITypeAdapter VarIntTypeAdapter = new BigIntegerTypeAdapter();
        public static ITypeAdapter CustomTypeAdapter = new DynamicCompositeTypeAdapter();
    }

    public class NullTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(byte[]);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            return decimalBuf;
        }

        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<byte[]>(value);
            return (byte[])value;
        }
    }

    public class DecimalTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(decimal);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            if (decimalBuf.Length > 16)
                throw new ArgumentOutOfRangeException("this java.math.BigDecimal is too big to fit into System.Decimal. Think about using other TypeAdapter for java.math.BigDecimal (e.g. J#, IKVM,...)");            
            
            byte[] scaleBytes = new byte[4];
            for(int i = 0; i < 4 ; i++)
                scaleBytes[i] = decimalBuf[i];

            byte[] bigIntBytes = new byte[decimalBuf.Length - 4];
            for(int i = 0; i < bigIntBytes.Length; i++)
                bigIntBytes[i] = decimalBuf[i+4];

            bool isNegative = (bigIntBytes[0] & 0x80) != 0;

            byte[] bytes = new byte[12];
            if (isNegative)
                for (int i = 0; i < 12; i++)
                    bytes[i] = 0xff;

            var offset = 12 - bigIntBytes.Length;
            for (int i = 0; i < bigIntBytes.Length; i++)
                bytes[offset + i] = bigIntBytes[i];
            
            byte[] lowB = new byte[4];
            byte[] midB = new byte[4];
            byte[] highB = new byte[4];

            Array.Copy(bytes, 8, lowB, 0, 4);
            Array.Copy(bytes, 4, midB, 0, 4);
            Array.Copy(bytes, 0, highB, 0, 4);

            Array.Reverse(lowB);
            Array.Reverse(midB);            
            Array.Reverse(highB);

            uint low = BitConverter.ToUInt32(lowB, 0);
            uint mid = BitConverter.ToUInt32(midB, 0);
            uint high = BitConverter.ToUInt32(highB, 0);
            byte scale = (byte)TypeInterpreter.BytesToInt32(scaleBytes, 0);

            if (isNegative)
            {
                low = ~low;
                mid = ~mid;
                high = ~high;
                
                high += (mid == 0xFFFFFFF && low == 0xFFFFFFF) ? 1u : 0;
                mid += (low == 0xFFFFFFF) ? 1u : 0;
                low += 1;
            }               
            return new decimal((int)low, (int)mid, (int)high, isNegative, scale);
        }


        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<decimal>(value);
            int[] bits = decimal.GetBits((decimal)value);

            byte[] bytes = new byte[16];

            int scale = (bits[3] >> 16) & 31;
            
            byte[] scaleB = BitConverter.GetBytes(scale);
            byte[] lowB = BitConverter.GetBytes(bits[0]);
            byte[] midB = BitConverter.GetBytes(bits[1]);
            byte[] highB = BitConverter.GetBytes(bits[2]);

            Array.Copy(lowB, 0, bytes, 0, 4);
            Array.Copy(midB, 0, bytes, 4, 4); 
            Array.Copy(highB, 0, bytes, 8, 4);
            Array.Copy(scaleB, 0, bytes,12, 4);

            if ((decimal)value < 0)
            {
                for (int i = 0; i < 12; i++)
                    bytes[i] = (byte)~bytes[i];
                bytes[0] += 1;
            }
            Array.Reverse(bytes);
            return bytes;
        }
    }

    public class BigIntegerTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(BigInteger);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            return new BigInteger(decimalBuf);
        }

        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<BigInteger>(value);
            return ((BigInteger)value).ToByteArray();
        }
    }

    public class DynamicCompositeTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(byte[]);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            return decimalBuf;
        }

        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<byte[]>(value);
            return (byte[])value;
        }
    }
}
