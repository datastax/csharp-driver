/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// based on java version by Dain Sundstrom (https://github.com/dain)
// ported to C# from https://github.com/dain/snappy/blob/master/src/main/java/org/iq80/snappy/SlowMemory.java

using System;

namespace Snappy
{
    internal class SlowMemory : IMemory
    {
        public bool FastAccessSupported()
        {
            return false;
        }


        public int LookupShort(short[] data, int index)
        {
            return data[index] & 0xFFFF;
        }

        public int LoadByte(byte[] data, int index)
        {
            return data[index] & 0xFF;
        }

        public int LoadInt(byte[] data, int index)
        {
            return (data[index] & 0xff) |
                   (data[index + 1] & 0xff) << 8 |
                   (data[index + 2] & 0xff) << 16 |
                   (data[index + 3] & 0xff) << 24;
        }

        public void CopyLong(byte[] src, int srcIndex, byte[] dest, int destIndex)
        {
            for (int i = 0; i < 8; i++)
            {
                dest[destIndex + i] = src[srcIndex + i];
            }
        }

        public long LoadLong(byte[] data, int index)
        {
            return (data[index] & 0xffL) |
                   (data[index + 1] & 0xffL) << 8 |
                   (data[index + 2] & 0xffL) << 16 |
                   (data[index + 3] & 0xffL) << 24 |
                   (data[index + 4] & 0xffL) << 32 |
                   (data[index + 5] & 0xffL) << 40 |
                   (data[index + 6] & 0xffL) << 48 |
                   (data[index + 7] & 0xffL) << 56;
        }

        public void CopyMemory(byte[] input, int inputIndex, byte[] output, int outputIndex, int length)
        {
            Buffer.BlockCopy(input, inputIndex, output, outputIndex, length);
        }
    }
} // end namespace