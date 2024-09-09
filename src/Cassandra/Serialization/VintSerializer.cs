// Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

namespace Cassandra.Serialization
{
    internal static class VintSerializer
    {
        private static long EncodeZigZag64(long n)
        {
            return (n << 1) ^ (n >> 63);
        }

        private static long DecodeZigZag64(long n)
        {
            return ((long)((ulong)n >> 1)) ^ -(n & 1);
        }

        private static int ComputeUnsignedVIntSize(long value)
        {
            var magnitude = LeadingZeros((ulong)(value | 1L));
            return (639 - magnitude * 9) >> 6;
        }

        private static int LeadingZeros(uint value)
        {
            var counter = 0;
            while (value != 0)
            {
                value = (value >> 1);
                counter++;
            }

            return 32 - counter;
        }

        private static int LeadingZeros(ulong value)
        {
            var counter = 0;
            while (value != 0)
            {
                value = value >> 1;
                counter++;
            }

            return 64 - counter;
        }

        private static int FirstByteValueMask(int extraBytesToRead)
        {
            return 0xff >> extraBytesToRead;
        }

        private static int EncodeExtraBytesToRead(int extraBytesToRead)
        {
            return ~FirstByteValueMask(extraBytesToRead);
        }

        private static int NumberOfExtraBytesToRead(int firstByte)
        {
            return LeadingZeros((uint)~firstByte << 24);
        }

        private static void EncodeVInt(long value, int size, byte[] buffer)
        {
            var extraBytes = size - 1;
            for (var i = extraBytes; i >= 0; --i)
            {
                buffer[i] = (byte)value;
                value >>= 8;
            }
            buffer[0] |= (byte)EncodeExtraBytesToRead(extraBytes);
        }

        public static int WriteUnsignedVInt(long value, byte[] buffer)
        {
            var size = ComputeUnsignedVIntSize(value);
            if (size == 1)
            {
                buffer[0] = (byte)value;
                return size;
            }
            EncodeVInt(value, size, buffer);
            return size;
        }

        public static int WriteVInt(long value, byte[] buffer)
        {
            return WriteUnsignedVInt(EncodeZigZag64(value), buffer);
        }

        public static long ReadUnsignedVInt(byte[] input, ref int offset)
        {
            var firstByte = input[offset++];
            if ((firstByte & 0x80) == 0)
            {
                return firstByte;
            }
            var size = NumberOfExtraBytesToRead(firstByte);
            long result = firstByte & FirstByteValueMask(size);
            for (var ii = 0; ii < size; ii++)
            {
                long b = input[offset++];
                result <<= 8;
                result |= b & 0xff;
            }
            return result;
        }

        public static long ReadVInt(byte[] buffer, ref int offset)
        {
            return DecodeZigZag64(ReadUnsignedVInt(buffer, ref offset));
        }
    }
}