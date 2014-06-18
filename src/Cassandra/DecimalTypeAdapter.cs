using System;

namespace Cassandra
{
    using System.Linq;

    public class DecimalTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof (decimal);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            // Although .NET decimals are stored on 16 bytes, 
            // their corresponding java.Math.BigDecimal objects can be stored on 17 bytes if the sign bit caused an overflow.
            bool isValidOverflow = decimalBuf.Length == 17 && (decimalBuf[4] == 0 || decimalBuf[4] == 255);

            if (decimalBuf.Length > 16 && !isValidOverflow)
                throw new ArgumentOutOfRangeException(
                    "this java.math.BigDecimal is too big to fit into System.Decimal. Think about using other TypeAdapter for java.math.BigDecimal (e.g. J#, IKVM,...)");

            bool isNegative = (decimalBuf[4] & 0x80) != 0;

            // drop the overflow byte from the buffer before processing
            if (isValidOverflow)
            {
                var buffer = new byte[16];
                Array.Copy(decimalBuf, 0, buffer, 0, 4);
                Array.Copy(decimalBuf, 5, buffer, 4, 12);
                decimalBuf = buffer;
            }

            var scaleBytes = new byte[4];
            Array.Copy(decimalBuf, 0, scaleBytes, 0, 4);

            var bufferLength = decimalBuf.Length - 4;
            var bigIntBytes = new byte[bufferLength];
            Array.Copy(decimalBuf, 4, bigIntBytes, 0, bufferLength);

            var bytes = new byte[12];
            if (isNegative)
                for (int i = 0; i < 12; i++)
                    bytes[i] = 0xff;

            int offset = 12 - bigIntBytes.Length;
            for (int i = 0; i < bigIntBytes.Length; i++)
                bytes[offset + i] = bigIntBytes[i];

            var lowB = new byte[4];
            var midB = new byte[4];
            var highB = new byte[4];

            Array.Copy(bytes, 8, lowB, 0, 4);
            Array.Copy(bytes, 4, midB, 0, 4);
            Array.Copy(bytes, 0, highB, 0, 4);

            Array.Reverse(lowB);
            Array.Reverse(midB);
            Array.Reverse(highB);

            uint low = BitConverter.ToUInt32(lowB, 0);
            uint mid = BitConverter.ToUInt32(midB, 0);
            uint high = BitConverter.ToUInt32(highB, 0);
            var scale = (byte) TypeInterpreter.BytesToInt32(scaleBytes, 0);

            if (isNegative)
            {
                low = ~low;
                mid = ~mid;
                high = ~high;

                high += (mid == 0xFFFFFFF && low == 0xFFFFFFF) ? 1u : 0;
                mid += (low == 0xFFFFFFF) ? 1u : 0;
                low += 1;
            }
            return new decimal((int) low, (int) mid, (int) high, isNegative, scale);
        }


        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<decimal>(value);
            int[] bits = decimal.GetBits((decimal) value);

            var bytes = new byte[16];

            int scale = (bits[3] >> 16) & 31;

            byte[] scaleB = BitConverter.GetBytes(scale);
            byte[] lowB = BitConverter.GetBytes(bits[0]);
            byte[] midB = BitConverter.GetBytes(bits[1]);
            byte[] highB = BitConverter.GetBytes(bits[2]);

            Array.Copy(lowB, 0, bytes, 0, 4);
            Array.Copy(midB, 0, bytes, 4, 4);
            Array.Copy(highB, 0, bytes, 8, 4);
            Array.Copy(scaleB, 0, bytes, 12, 4);

            // in java.Math.BigDecimal, the role of the bit checked below is to indicate that the number is negative.
            // if the value overlaps the sign bit, we need to insert a new byte into the array.
            if ((highB[3] & 0x80) != 0)
            {
                var extendedBytes = new byte[17];
                Array.Copy(bytes, 0, extendedBytes, 0, 12);
                extendedBytes[12] = 0;
                Array.Copy(bytes, 12, extendedBytes, 13, 4);
                bytes = extendedBytes;
            }

            if ((decimal) value < 0)
            {
                for (int i = 0; i < bytes.Length-4; i++)
                    bytes[i] = (byte) ~bytes[i];
                bytes[0] += 1;
            }
            Array.Reverse(bytes);
            return bytes;
        }
    }
}