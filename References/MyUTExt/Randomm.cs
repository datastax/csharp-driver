using System;
using System.Collections.Generic;
using System.Text;

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
    }
}
