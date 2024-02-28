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
using System.Reflection;

namespace Cassandra.IntegrationTests.TestBase
{
    internal class Randomm
    {
        [ThreadStatic] private static Randomm _rnd;

        private readonly Random _r;

        public static Randomm Instance
        {
            get { return Randomm._rnd ?? (Randomm._rnd = new Randomm(5)); }
        }

        private Randomm(int seed)
        {
            _r = new Random(seed);
        }

        internal static object RandomVal(Type tp)
        {
            if (tp != null)
                return Randomm.Instance.GetType().GetTypeInfo().GetMethod("Next" + tp.Name).Invoke(Randomm.Instance, new object[] {});
            return "";
        }

        public int Next()
        {
            return _r.Next();
        }

        public int Next(int minValue, int maxValue)
        {
            return _r.Next(minValue, maxValue);
        }

        public int Next(int maxValue)
        {
            return _r.Next(maxValue);
        }

        public double NextDouble()
        {
            return _r.NextDouble();
        }

        public float NextSingle()
        {
            double numb = _r.NextDouble();
            numb -= 0.5;
            numb *= 2;
            return float.MaxValue*(float) numb;
        }

        public UInt16 NextUInt16()
        {
            return (ushort) _r.Next(0, 65535);
        }

        public static int NextInt32()
        {
            return Randomm.Instance.Next();
        }

        public Int64 NextInt64()
        {
            var buffer = new byte[sizeof (Int64)];
            _r.NextBytes(buffer);
            return BitConverter.ToInt64(buffer, 0);
        }

        public decimal NextDecimal()
        {
            var scale = (byte) _r.Next(29);
            bool sign = _r.Next(2) == 1;

            return new decimal(Randomm.NextInt32(),
                               Randomm.NextInt32(),
                               Randomm.NextInt32(),
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
            _r.NextBytes(btarr);
            return btarr;
        }

        public void NextBytes(byte[] buffer)
        {
            _r.NextBytes(buffer);
        }

        public System.Net.IPAddress NextIPAddress()
        {
            byte[] btarr = new byte[]{(byte)this.Next(0, 128), (byte)this._r.Next(0, 128), (byte)this._r.Next(0, 128), (byte)this._r.Next(0, 128)};
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

        public static int RandomInt()
        {
            return Randomm.NextInt32();
        }

        public static string RandomAlphaNum(int strLen)
        {
            string randomStr = "";
            while (randomStr.Length < strLen)
            {
                randomStr += Guid.NewGuid().ToString().Replace("-", "");
                if (randomStr.Length > strLen)
                    randomStr = randomStr.Substring(0, strLen);
            }
            return randomStr;
        }
    }
}
