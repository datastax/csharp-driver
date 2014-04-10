/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ported to C# from https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/MurmurHash.java

/// <summary>
/// This is a very fast, non-cryptographic hash suitable for general hash-based
/// lookup. See http://murmurhash.googlepages.com/ for more details.
/// Hash3_x64_128() is MurmurHash 3.0.
/// 
/// The C version of MurmurHash 2.0 found at that site was ported to Java by
/// Andrzej Bialecki (ab at getopt org).
/// </summary>
internal class MurmurHash
{
    private static long GetBlock(byte[] key, int offset, int index)
    {
        int i8 = index << 3;
        int blockOffset = offset + i8;
        return ((long) key[blockOffset + 0] & 0xff) + (((long) key[blockOffset + 1] & 0xff) << 8) +
               (((long) key[blockOffset + 2] & 0xff) << 16) + (((long) key[blockOffset + 3] & 0xff) << 24) +
               (((long) key[blockOffset + 4] & 0xff) << 32) + (((long) key[blockOffset + 5] & 0xff) << 40) +
               (((long) key[blockOffset + 6] & 0xff) << 48) + (((long) key[blockOffset + 7] & 0xff) << 56);
    }

    private static long Rotl64(long v, int n)
    {
        return ((v << n) | ((long) ((ulong) v >> (64 - n))));
    }

    private static long Fmix(long k)
    {
        k ^= (long) ((ulong) k >> 33);
        k *= -0xAE502812AA7333;
        k ^= (long) ((ulong) k >> 33);
        k *= -0x3B314601E57A13AD;
        k ^= (long) ((ulong) k >> 33);

        return k;
    }

    public static long[] Hash3_x64_128(byte[] key, int offset, int length, long seed)
    {
        int nblocks = length >> 4; // Process as 128-bit blocks.

        long h1 = seed;
        long h2 = seed;

        const long c1 = -0x783C846EEEBDAC2B;
        const long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for (int i = 0; i < nblocks; i++)
        {
            long k1 = GetBlock(key, offset, i*2 + 0);
            long k2 = GetBlock(key, offset, i*2 + 1);

            k1 *= c1;
            k1 = Rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = Rotl64(h1, 27);
            h1 += h2;
            h1 = h1*5 + 0x52dce729;

            k2 *= c2;
            k2 = Rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = Rotl64(h2, 31);
            h2 += h1;
            h2 = h2*5 + 0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks*16;

        {
            long k1 = 0;
            long k2 = 0;

            switch (length & 15)
            {
                case 15:
                    k2 ^= ((long) key[offset + 14]) << 48;
                    goto case 14;
                case 14:
                    k2 ^= ((long) key[offset + 13]) << 40;
                    goto case 13;
                case 13:
                    k2 ^= ((long) key[offset + 12]) << 32;
                    goto case 12;
                case 12:
                    k2 ^= ((long) key[offset + 11]) << 24;
                    goto case 11;
                case 11:
                    k2 ^= ((long) key[offset + 10]) << 16;
                    goto case 10;
                case 10:
                    k2 ^= ((long) key[offset + 9]) << 8;
                    goto case 9;
                case 9:
                    k2 ^= ((long) key[offset + 8]) << 0;
                    k2 *= c2;
                    k2 = Rotl64(k2, 33);
                    k2 *= c1;
                    h2 ^= k2;
                    goto case 8;

                case 8:
                    k1 ^= ((long) key[offset + 7]) << 56;
                    goto case 7;
                case 7:
                    k1 ^= ((long) key[offset + 6]) << 48;
                    goto case 6;
                case 6:
                    k1 ^= ((long) key[offset + 5]) << 40;
                    goto case 5;
                case 5:
                    k1 ^= ((long) key[offset + 4]) << 32;
                    goto case 4;
                case 4:
                    k1 ^= ((long) key[offset + 3]) << 24;
                    goto case 3;
                case 3:
                    k1 ^= ((long) key[offset + 2]) << 16;
                    goto case 2;
                case 2:
                    k1 ^= ((long) key[offset + 1]) << 8;
                    goto case 1;
                case 1:
                    k1 ^= key[offset];
                    k1 *= c1;
                    k1 = Rotl64(k1, 31);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
            }
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
        h2 += h1;

        return (new[] {h1, h2});
    }
}