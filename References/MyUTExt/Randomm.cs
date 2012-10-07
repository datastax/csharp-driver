using System;
using System.Collections.Generic;
using System.Text;
using System.Numerics;

namespace MyUTExt
{
    class Randomm : Random
    {
        public Randomm()
            : base(5)
        {
        }

        public float NextSingle()
        {
            double numb = this.NextDouble();
            numb -= 0.5;
            numb *= 2;
            return float.MaxValue * (float)numb;
        }
        public int NextInt32()
        {
            return this.Next();
        }
        public Int64 NextInt64()
        {
            var buffer = new byte[sizeof(Int64)];
            this.NextBytes(buffer);
            return BitConverter.ToInt64(buffer, 0);
        }
        public decimal NextDecimal()
        {
            byte scale = (byte)this.Next(29);
            bool sign = this.Next(2) == 1;
            return new decimal(this.NextInt32(),
                               this.NextInt32(),
                               this.NextInt32(),
                               sign,
                               scale);
        }
        public BigInteger NextBigInteger()
        {
            return new BigInteger(Int64.MaxValue) * 10;
        }

        public string NextChar()
        {            
            string asciiString = String.Empty;
            for (int i = 0; i < 128; i++)
                if (i == 34 || i == 39)
                    continue;
                else 
                    asciiString += (char)i;

            return asciiString;
        }


    }
}
