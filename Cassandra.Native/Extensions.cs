using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Numerics;
using Cassandra.Native;

namespace Cassandra
{
    public static class Extensions
    {
        public static BigInteger ToBigInteger(this VarintBuffer varint)
        {
            return new BigInteger(varint.BigIntegerBytes);
        }

        public static VarintBuffer ToVarintBuffer(this BigInteger value)
        {
            return new VarintBuffer() { BigIntegerBytes = value.ToByteArray() };
        }


        public static BigDecimal ToDecimal(this DecimalBuffer decim)
        {
            return new BigDecimal(decim.BigIntegerBytes);
        }

        public static DecimalBuffer ToDecimalBuffer(this BigDecimal value)
        {
            return new DecimalBuffer() { BigIntegerBytes = value.ToByteArray()};            
        }

    }
}
