using System;
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
