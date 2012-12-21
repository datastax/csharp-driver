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


/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 *
 * hash3_x64_128() is MurmurHash 3.0.
 *
 */
internal class MurmurHash
{

    protected static ulong getblock(byte[] key, uint offset, uint index)
    {
        uint i_8 = index << 3;
        uint blockOffset = offset + i_8;
        return ((ulong)key[blockOffset + 0] & 0xff) + (((ulong)key[blockOffset + 1] & 0xff) << 8) +
               (((ulong)key[blockOffset + 2] & 0xff) << 16) + (((ulong)key[blockOffset + 3] & 0xff) << 24) +
               (((ulong)key[blockOffset + 4] & 0xff) << 32) + (((ulong)key[blockOffset + 5] & 0xff) << 40) +
               (((ulong)key[blockOffset + 6] & 0xff) << 48) + (((ulong)key[blockOffset + 7] & 0xff) << 56);
    }

    protected static ulong rotl64(ulong v, int n)
    {
        return ((v << n) | (v >> (64 - n)));
    }

    protected static ulong fmix(ulong k)
    {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >> 33;

        return k;
    }

    public static ulong[] hash3_x64_128(byte[] key, uint offset, uint length, ulong seed)
    {
        uint nblocks = length >> 4; // Process as 128-bit blocks.

        ulong h1 = seed;
        ulong h2 = seed;

        ulong c1 = 0x87c37b91114253d5L;
        ulong c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for (uint i = 0; i < nblocks; i++)
        {
            ulong k1 = getblock(key, offset, i * 2 + 0);
            ulong k2 = getblock(key, offset, i * 2 + 1);

            k1 *= c1; k1 = rotl64(k1, 31); k1 *= c2; h1 ^= k1;

            h1 = rotl64(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;

            k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2;

            h2 = rotl64(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks * 16;

        {
            ulong k1 = 0;
            ulong k2 = 0;

            switch (length & 15)
            {
                case 15: k2 ^= ((ulong)key[offset + 14]) << 48; goto case 14;
                case 14: k2 ^= ((ulong)key[offset + 13]) << 40; goto case 13;
                case 13: k2 ^= ((ulong)key[offset + 12]) << 32; goto case 12;
                case 12: k2 ^= ((ulong)key[offset + 11]) << 24; goto case 11;
                case 11: k2 ^= ((ulong)key[offset + 10]) << 16; goto case 10;
                case 10: k2 ^= ((ulong)key[offset + 9]) << 8; goto case 9;
                case 9: k2 ^= ((ulong)key[offset + 8]) << 0;
                    k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2; 
                    goto case 8;

                case 8: k1 ^= ((ulong)key[offset + 7]) << 56; goto case 7;
                case 7: k1 ^= ((ulong)key[offset + 6]) << 48; goto case 6;
                case 6: k1 ^= ((ulong)key[offset + 5]) << 40; goto case 5;
                case 5: k1 ^= ((ulong)key[offset + 4]) << 32; goto case 4;
                case 4: k1 ^= ((ulong)key[offset + 3]) << 24; goto case 3;
                case 3: k1 ^= ((ulong)key[offset + 2]) << 16; goto case 2;
                case 2: k1 ^= ((ulong)key[offset + 1]) << 8; goto case 1;
                case 1: k1 ^= ((ulong)key[offset]);
                    k1 *= c1; k1 = rotl64(k1, 31); k1 *= c2; h1 ^= k1; 
                    break;
            };
        }
        //----------
        // finalization

        h1 ^= length; h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        return (new ulong[] { h1, h2 });
    }
}
