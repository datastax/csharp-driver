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

using System;
using System.Numerics;

namespace Cassandra.IntegrationTests.Core
{
    internal class Randomm : Random
    {
        [ThreadStatic] private static Randomm _rnd;

        public static Randomm Instance
        {
            get { return _rnd ?? (_rnd = new Randomm(5)); }
        }

        private Randomm(int seed) : base(seed)
        {
        }

        internal static object RandomVal(Type tp)
        {
            if (tp != null)
                return Instance.GetType().GetMethod("Next" + tp.Name).Invoke(Instance, new object[] {});
            return "";
        }

        public float NextSingle()
        {
            double numb = NextDouble();
            numb -= 0.5;
            numb *= 2;
            return float.MaxValue*(float) numb;
        }

        public UInt16 NextUInt16()
        {
            return (ushort) Next(0, 65535);
        }

        public int NextInt32()
        {
            return Next();
        }

        public Int64 NextInt64()
        {
            var buffer = new byte[sizeof (Int64)];
            NextBytes(buffer);
            return BitConverter.ToInt64(buffer, 0);
        }

        public decimal NextDecimal()
        {
            var scale = (byte) Next(29);
            bool sign = Next(2) == 1;

            return new decimal(NextInt32(),
                               NextInt32(),
                               NextInt32(),
                               sign,
                               scale);
        }

        public BigInteger NextBigInteger()
        {
            return new BigInteger(Int64.MaxValue)*10;
        }

        public string NextString()
        {
            return NextChar();
        }

        public string NextChar()
        {
            string asciiString = string.Empty;
            for (int i = 0; i < 128; i++)
                if (i == 34 || i == 39)
                    continue;
                else
                    asciiString += (char) i;

            return asciiString;
        }

        public DateTimeOffset NextDateTimeOffset()
        {
            DateTime now = DateTimeOffset.Now.UtcDateTime;
            return new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Millisecond, TimeSpan.Zero);
        }

        public byte[] NextByte()
        {
            var btarr = new byte[NextUInt16()];
            NextBytes(btarr);
            return btarr;
        }

        public System.Net.IPAddress NextIPAddress()
        {
            byte[] btarr = new byte[]{(byte)this.Next(0, 128), (byte)this.Next(0, 128), (byte)this.Next(0, 128), (byte)this.Next(0, 128)};
            return new System.Net.IPAddress(btarr);
        }

        public bool NextBoolean()
        {
            return NextUInt16() > 127 ? true : false;
        }

        public Guid NextGuid()
        {
            return Guid.NewGuid();
        }
    }
}