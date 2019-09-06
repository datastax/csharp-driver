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

namespace Cassandra
{
    internal class M3PToken : IToken
    {
        public static readonly TokenFactory Factory = new M3PTokenFactory();
        private readonly long _value;

        internal M3PToken(long value)
        {
            _value = value;
        }

        public int CompareTo(object obj)
        {
            var other = obj as M3PToken;
            long otherValue = other._value;
            return _value < otherValue ? -1 : (_value == otherValue) ? 0 : 1;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || GetType() != obj.GetType())
                return false;

            return _value == ((M3PToken) obj)._value;
        }

        public override int GetHashCode()
        {
            return (int) (_value ^ ((long) ((ulong) _value >> 32)));
        }

        public override string ToString()
        {
            return _value.ToString();
        }

        internal class M3PTokenFactory : TokenFactory
        {
            public override IToken Parse(string tokenStr)
            {
                return new M3PToken(long.Parse(tokenStr));
            }

            public override IToken Hash(byte[] partitionKey)
            {
                var v = Murmur(partitionKey);
                return new M3PToken(v == long.MinValue ? long.MaxValue : v);
            }

            /// <summary>
            /// Murmur hash it
            /// </summary>
            /// <returns></returns>
            private static long Murmur(byte[] bytes)
            {
                // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
                // for M3P. Compared to that methods, there's a few inlining of arguments and we
                // only return the first 64-bits of the result since that's all M3P uses.

                //Convert to sbyte as in Java byte are signed
                sbyte[] data = (sbyte[])(Array)bytes;

                int offset = 0;
                int length = data.Length;

                int nblocks = length >> 4; // Process as 128-bit blocks.

                long h1 = 0;
                long h2 = 0;

                //Instead of using ulong for constants, use long values representing the same bits
                //Negated, same bits as ulong: 0x87c37b91114253d5L
                const long c1 = -0x783C846EEEBDAC2BL;
                const long c2 = 0x4cf5ad432745937fL;

                //----------
                // body

                for (int i = 0; i < nblocks; i++)
                {
                    long k1 = GetBlock(data, offset, i * 2 + 0);
                    long k2 = GetBlock(data, offset, i * 2 + 1);

                    k1 *= c1; 
                    k1 = Rotl64(k1, 31); 
                    k1 *= c2; 
                    
                    h1 ^= k1;
                    h1 = Rotl64(h1, 27); 
                    h1 += h2; 
                    h1 = h1 * 5 + 0x52dce729;

                    k2 *= c2; 
                    k2 = Rotl64(k2, 33); 
                    k2 *= c1; h2 ^= k2;
                    h2 = Rotl64(h2, 31); 
                    h2 += h1; 
                    h2 = h2 * 5 + 0x38495ab5;
                }

                //----------
                // tail

                // Advance offset to the unprocessed tail of the data.
                offset += nblocks * 16;
                {
                    //context
                    long k1 = 0;
                    long k2 = 0;

                    switch (length & 15)
                    {
                        case 15: 
                            k2 ^= ((long)data[offset + 14]) << 48;
                            goto case 14;
                        case 14: 
                            k2 ^= ((long)data[offset + 13]) << 40;
                            goto case 13;
                        case 13: 
                            k2 ^= ((long)data[offset + 12]) << 32;
                            goto case 12;
                        case 12: 
                            k2 ^= ((long)data[offset + 11]) << 24;
                            goto case 11;
                        case 11: 
                            k2 ^= ((long)data[offset + 10]) << 16;
                            goto case 10;
                        case 10: 
                            k2 ^= ((long)data[offset + 9 ]) << 8;
                            goto case 9;
                        case 9:  
                            k2 ^= ((long)data[offset + 8 ]) << 0;
                            k2 *= c2; 
                            k2 = Rotl64(k2, 33); 
                            k2 *= c1; 
                            h2 ^= k2;
                            goto case 8;
                        case 8: 
                            k1 ^= ((long)data[offset + 7]) << 56;
                            goto case 7;
                        case 7: 
                            k1 ^= ((long)data[offset + 6]) << 48;
                            goto case 6;
                        case 6: 
                            k1 ^= ((long)data[offset + 5]) << 40;
                            goto case 5;
                        case 5: 
                            k1 ^= ((long)data[offset + 4]) << 32;
                            goto case 4;
                        case 4: 
                            k1 ^= ((long)data[offset + 3]) << 24;
                            goto case 3;
                        case 3: 
                            k1 ^= ((long)data[offset + 2]) << 16;
                            goto case 2;
                        case 2: 
                            k1 ^= ((long)data[offset + 1]) << 8;
                            goto case 1;
                        case 1: 
                            k1 ^= ((long)data[offset]);
                            k1 *= c1; 
                            k1 = Rotl64(k1, 31); 
                            k1 *= c2;
                            h1 ^= k1;
                            break;
                    }

                    //----------
                    // finalization

                    h1 ^= length; 
                    h2 ^= length;

                    h1 += h2;
                    h2 += h1;

                    h1 = Fmix(h1);
                    h2 = Fmix(h2);

                    h1 += h2;

                    return h1;
                }
            }

            private static long GetBlock(sbyte[] key, int offset, int index)
            {
                int i8 = index << 3;
                int blockOffset = offset + i8;
                return ((long)key[blockOffset + 0] & 0xff) + (((long)key[blockOffset + 1] & 0xff) << 8) +
                       (((long)key[blockOffset + 2] & 0xff) << 16) + (((long)key[blockOffset + 3] & 0xff) << 24) +
                       (((long)key[blockOffset + 4] & 0xff) << 32) + (((long)key[blockOffset + 5] & 0xff) << 40) +
                       (((long)key[blockOffset + 6] & 0xff) << 48) + (((long)key[blockOffset + 7] & 0xff) << 56);
            }

            private static long Rotl64(long v, int n)
            {
                return ((v << n) | ((long)((ulong)v >> (64 - n))));
            }

            private static long Fmix(long k)
            {
                k ^= (long)((ulong)k >> 33);
                //Negated, same bits as ulong 0xff51afd7ed558ccdL
                k *= -0xAE502812AA7333;
                k ^= (long)((ulong)k >> 33);
                //Negated, same bits as ulong 0xc4ceb9fe1a85ec53L
                k *= -0x3B314601E57A13AD;
                k ^= (long)((ulong)k >> 33);
                return k;
            }
        }
    }
}