//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Serialization;

namespace Dse
{
    using System.Numerics;

    public class DecimalTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof (decimal);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            var bigintBytes = new byte[decimalBuf.Length - 4];
            Array.Copy(decimalBuf, 4, bigintBytes, 0, bigintBytes.Length);
            //Scale representation is an int, but System.Decimal only supports a scale of 1 byte
            var scale = decimalBuf[3];

            Array.Reverse(bigintBytes);
            var bigInteger = new BigInteger(bigintBytes);
            var isNegative = bigInteger < 0;

            bigInteger = BigInteger.Abs(bigInteger);
            bigintBytes = bigInteger.ToByteArray();
            if (bigintBytes.Length > 13 || (bigintBytes.Length == 13 && bigintBytes[12] != 0))
            {
                throw new ArgumentOutOfRangeException(
                    "decimalBuf",
                    "this java.math.BigDecimal is too big to fit into System.Decimal. Think about using other TypeAdapter for java.math.BigDecimal (e.g. J#, IKVM,...)");
            }

            var intArray = new int[3];
            Buffer.BlockCopy(bigintBytes, 0, intArray, 0, Math.Min(12, bigintBytes.Length));

            return new decimal(intArray[0], intArray[1], intArray[2], isNegative, scale);
        }


        public byte[] ConvertTo(object value)
        {
            TypeSerializer.CheckArgument<decimal>(value);
            var decimalValue = (decimal)value;
            int[] bits = decimal.GetBits(decimalValue);

            int scale = (bits[3] >> 16) & 31;

            byte[] scaleBytes = BeConverter.GetBytes(scale);

            var bigintBytes = new byte[13]; // 13th byte is for making sure that the number is positive
            Buffer.BlockCopy(bits, 0, bigintBytes, 0, 12);

            var bigInteger = new BigInteger(bigintBytes);
            if (decimalValue < 0)
            {
                bigInteger = -bigInteger;
            }

            bigintBytes = bigInteger.ToByteArray();
            Array.Reverse(bigintBytes);

            var resultBytes = new byte[scaleBytes.Length + bigintBytes.Length];
            Array.Copy(scaleBytes, resultBytes, scaleBytes.Length);
            Array.Copy(bigintBytes, 0, resultBytes, scaleBytes.Length, bigintBytes.Length);
            return resultBytes;
        }
    }
}