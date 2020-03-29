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
    /// <summary>
    /// Equivalent of <see cref="System.BitConverter"/> but let's you choose the endianness.
    /// </summary>
    internal static class EndianBitConverter
    {
        /// <summary>
        /// Converts an int into an array of bytes and sets to the buffer starting at the specified offset.
        /// </summary>
        internal static void SetBytes(bool isLittleEndian, byte[] buffer, int offset, int value)
        {
            if (isLittleEndian)
            {
                buffer[offset] = (byte)(value & 0xFF);
                buffer[offset + 1] = (byte)((value & 0xFF00) >> 8);
                buffer[offset + 2] = (byte)((value & 0xFF0000) >> 16);
                buffer[offset + 3] = (byte)((value & 0xFF000000) >> 24);
                return;
            }
            buffer[offset] = (byte) ((value & 0xFF000000) >> 24);
            buffer[offset + 1] = (byte) ((value & 0xFF0000) >> 16);
            buffer[offset + 2] = (byte) ((value & 0xFF00) >> 8);
            buffer[offset + 3] = (byte) (value & 0xFF);
        }

        /// <summary>
        /// Converts an 64-bit double into an array of bytes and sets to the buffer starting at the specified offset.
        /// </summary>
        internal static void SetBytes(bool isLittleEndian, byte[] buffer, int offset, double value)
        {
            SetBytes(isLittleEndian, buffer, offset, BitConverter.DoubleToInt64Bits(value));
        }

        /// <summary>
        /// Converts an int into an array of bytes and sets to the buffer starting at the specified offset.
        /// </summary>
        internal static void SetBytes(bool isLittleEndian, byte[] buffer, int offset, long value)
        {
            if (isLittleEndian)
            {
                buffer[offset] = (byte)(value & 0xFF);
                buffer[offset + 1] = (byte)((value & 0xFF00) >> 8);
                buffer[offset + 2] = (byte)((value & 0xFF0000) >> 16);
                buffer[offset + 3] = (byte)((value & 0xFF000000) >> 24);
                buffer[offset + 4] = (byte)((value & 0xFF00000000) >> 32);
                buffer[offset + 5] = (byte)((value & 0xFF0000000000) >> 40);
                buffer[offset + 6] = (byte)((value & 0xFF000000000000) >> 48);
                buffer[offset + 7] = (byte)(((ulong)value & 0xFF00000000000000) >> 56);
                return;
            }
            buffer[offset] = (byte) (((ulong) value & 0xFF00000000000000) >> 56);
            buffer[offset + 1] = (byte) ((value & 0xFF000000000000) >> 48);
            buffer[offset + 2] = (byte) ((value & 0xFF0000000000) >> 40);
            buffer[offset + 3] = (byte) ((value & 0xFF00000000) >> 32);
            buffer[offset + 4] = (byte) ((value & 0xFF000000) >> 24);
            buffer[offset + 5] = (byte) ((value & 0xFF0000) >> 16);
            buffer[offset + 6] = (byte) ((value & 0xFF00) >> 8);
            buffer[offset + 7] = (byte) (value & 0xFF);
        }

        /// <summary>
        /// Returns a signed 32-bit integer from four bytes at specified offset from the buffer.
        /// </summary>
        internal static int ToInt32(bool isLittleEndian, byte[] buffer, int offset)
        {
            if (isLittleEndian)
            {
                return (buffer[offset + 3] << 24 | buffer[offset + 2] << 16 | buffer[offset + 1] << 8 | buffer[offset]);
            }
            return (buffer[offset] << 24 | buffer[offset + 1] << 16 | buffer[offset + 2] << 8 | buffer[offset + 3]);
        }

        /// <summary>
        /// Returns a signed 64-bit integer from eight bytes at specified offset from the buffer.
        /// </summary>
        public static long ToInt64(bool isLittleEndian, byte[] buffer, int offset)
        {
            if (isLittleEndian)
            {
                return (long)(
                      ((ulong)buffer[offset + 7] << 56)
                    | ((ulong)buffer[offset + 6] << 48)
                    | ((ulong)buffer[offset + 5] << 40)
                    | ((ulong)buffer[offset + 4] << 32)
                    | ((ulong)buffer[offset + 3] << 24)
                    | ((ulong)buffer[offset + 2] << 16)
                    | ((ulong)buffer[offset + 1] << 8)
                    | buffer[offset]
                );
            }
            return (long)(
                  ((ulong)buffer[offset] << 56)
                | ((ulong)buffer[offset + 1] << 48)
                | ((ulong)buffer[offset + 2] << 40)
                | ((ulong)buffer[offset + 3] << 32)
                | ((ulong)buffer[offset + 4] << 24)
                | ((ulong)buffer[offset + 5] << 16)
                | ((ulong)buffer[offset + 6] << 8)
                | buffer[offset + 7]
            );
        }

        /// <summary>
        /// Returns a signed 64-bit double from eight bytes at specified offset from the buffer.
        /// </summary>
        public static double ToDouble(bool isLittleEndian, byte[] buffer, int offset)
        {
            return BitConverter.Int64BitsToDouble(ToInt64(isLittleEndian, buffer, offset));
        }
    }
}
