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
    /// Contains methods for converting a big-endian array of bytes to one of the base data types, 
    /// as well as for converting a base data type to a big-endian array of bytes.
    /// </summary>
    internal static class BeConverter
    {
        /// <summary>
        /// Converts a short to a big endian byte array
        /// </summary>
        public static byte[] GetBytes(short value)
        {
            return new[] { (byte)((value & 0xFF00) >> 8), (byte) (value & 0xFF)};
        }

        /// <summary>
        /// Converts a ushort to a big endian byte array
        /// </summary>
        public static byte[] GetBytes(ushort value)
        {
            return GetBytes((short) value);
        }

        /// <summary>
        /// Converts an int to a big endian byte array
        /// </summary>
        public static byte[] GetBytes(int value)
        {
            return new[]
            {
                (byte) ((value & 0xFF000000) >> 24),
                (byte) ((value & 0xFF0000) >> 16),
                (byte) ((value & 0xFF00) >> 8),
                (byte) (value & 0xFF)
            };
        }

        /// <summary>
        /// Converts a long to a big endian byte array
        /// </summary>
        public static byte[] GetBytes(long value)
        {
            return new []
            {
                (byte) (((ulong) value & 0xFF00000000000000) >> 56),
                (byte) ((value & 0xFF000000000000) >> 48),
                (byte) ((value & 0xFF0000000000) >> 40),
                (byte) ((value & 0xFF00000000) >> 32),
                (byte) ((value & 0xFF000000) >> 24),
                (byte) ((value & 0xFF0000) >> 16),
                (byte) ((value & 0xFF00) >> 8),
                (byte) (value & 0xFF)
            };
        }

        /// <summary>
        /// Converts a double to a big endian byte array
        /// </summary>
        public static byte[] GetBytes(double value)
        {
            var buffer = BitConverter.GetBytes(value);
            if (!BitConverter.IsLittleEndian)
            {
                return buffer;
            }
            return new[] { buffer[7], buffer[6], buffer[5], buffer[4], buffer[3], buffer[2], buffer[1], buffer[0] };
        }

        /// <summary>
        /// Converts a double to a big endian byte array
        /// </summary>
        public static byte[] GetBytes(float value)
        {
            var buffer = BitConverter.GetBytes(value);
            if (!BitConverter.IsLittleEndian)
            {
                return buffer;
            }
            return new[] { buffer[3], buffer[2], buffer[1], buffer[0] };
        }

        /// <summary>
        /// Converts an big-endian array of bytes into a short.
        /// </summary>
        public static short ToInt16(byte[] value, int offset = 0)
        {
            return (short)((value[offset] << 8) | value[offset + 1]);
        }

        /// <summary>
        /// Converts an big-endian array of bytes into a ushort.
        /// </summary>
        public static ushort ToUInt16(byte[] value)
        {
            return (ushort)((value[0] << 8) | value[1]);
        }

        /// <summary>
        /// Converts an big-endian array of bytes into an int.
        /// </summary>
        public static int ToInt32(byte[] value, int offset = 0)
        {
            return (value[offset] << 24 | value[offset + 1] << 16 | value[offset + 2] << 8 | value[offset + 3]);
        }

        /// <summary>
        /// Converts an big-endian array of bytes into a long.
        /// </summary>
        public static long ToInt64(byte[] value, int offset = 0)
        {
            return (long)(
                  ((ulong)value[offset]     << 56)
                | ((ulong)value[offset + 1] << 48)
                | ((ulong)value[offset + 2] << 40)
                | ((ulong)value[offset + 3] << 32)
                | ((ulong)value[offset + 4] << 24)
                | ((ulong)value[offset + 5] << 16)
                | ((ulong)value[offset + 6] << 8)
                | (value[offset + 7])
            );
        }

        /// <summary>
        /// Converts an big-endian array of bytes into a double.
        /// </summary>
        public static double ToDouble(byte[] value, int offset = 0)
        {
            if (!BitConverter.IsLittleEndian)
            {
                return BitConverter.ToDouble(value, offset);
            }
            return BitConverter.ToDouble(new[]
            {
                //Invert the first 8 bytes, starting from offset
                value[offset + 7], value[offset + 6], value[offset + 5], value[offset + 4], value[offset + 3], value[offset + 2], value[offset + 1], value[offset]
            }, 0);
        }

        /// <summary>
        /// Converts an big-endian array of bytes into a float.
        /// </summary>
        public static float ToSingle(byte[] value, int offset = 0)
        {
            if (!BitConverter.IsLittleEndian)
            {
                return BitConverter.ToSingle(value, offset);
            }
            return BitConverter.ToSingle(new[]
            {
                //Invert the first 4 bytes, starting from offset
                value[offset + 3], value[offset + 2], value[offset + 1], value[offset]
            }, 0);
        }
    }
}
