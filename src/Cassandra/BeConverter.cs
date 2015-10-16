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
        /// Converts an short to a big endian byte array
        /// </summary>
        public static byte[] GetBytes(short value)
        {
            return new[] { (byte)((value & 0xFF00) >> 8), (byte) value };
        }

        /// <summary>
        /// Converts an ushort to a big endian byte array
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
        /// Converts an long to a big endian byte array
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
    }
}
