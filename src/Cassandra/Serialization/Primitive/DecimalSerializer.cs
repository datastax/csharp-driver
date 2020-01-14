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

namespace Cassandra.Serialization.Primitive
{
    /// <summary>
    /// Deprecated: this class will be made internal in the next major version.
    /// </summary>
    public class DecimalSerializer : TypeSerializer<decimal>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Decimal; }
        }

        public override decimal Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var scale = BeConverter.ToInt32(buffer, offset);
            var unscaledBytes = Utils.SliceBuffer(buffer, offset + 4, length - 4);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(unscaledBytes);
            }
            return ToDecimal(new BigInteger(unscaledBytes), scale);
        }

        internal static decimal ToDecimal(BigInteger unscaledValue, int scale)
        {
            if (scale < -28 || scale > 28)
            {
                //Scale representation is an int, but System.Decimal only supports a scale from -28 to 28
                throw new ArgumentOutOfRangeException(
                    "scale", scale, "CLR Decimal structure can not represent numbers with a scale greater than 28");
            }
            if (scale < 0)
            {
                try
                {
                    return (decimal) (unscaledValue*BigInteger.Pow(new BigInteger(10), Math.Abs(scale)));
                }
                catch (OverflowException)
                {
                    throw new ArgumentOutOfRangeException(
                        "unscaledValue",
                        unscaledValue*BigInteger.Pow(new BigInteger(10), Math.Abs(scale)),
                        "Value can not be represented as a CLR Decimal");
                }
            }
            var isNegative = unscaledValue < 0;
            unscaledValue = BigInteger.Abs(unscaledValue);
            var bigintBytes = unscaledValue.ToByteArray();
            if (bigintBytes.Length > 13 || (bigintBytes.Length == 13 && bigintBytes[12] != 0))
            {
                throw new ArgumentOutOfRangeException(
                    "unscaledValue", unscaledValue, "Value can not be represented as a CLR Decimal");
            }
            var intArray = new int[3];
            Buffer.BlockCopy(bigintBytes, 0, intArray, 0, Math.Min(12, bigintBytes.Length));
            return new decimal(intArray[0], intArray[1], intArray[2], isNegative, (byte)scale);
        }

        public override byte[] Serialize(ushort protocolVersion, decimal value)
        {
            int[] bits = decimal.GetBits(value);
            int scale = (bits[3] >> 16) & 31;
            byte[] scaleBytes = BeConverter.GetBytes(scale);
            var bigintBytes = new byte[13]; // 13th byte is for making sure that the number is positive
            Buffer.BlockCopy(bits, 0, bigintBytes, 0, 12);
            var bigInteger = new BigInteger(bigintBytes);
            if (value < 0)
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
