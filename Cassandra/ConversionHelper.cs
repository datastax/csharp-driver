using System;

//based on https://github.com/managedfusion/fluentcassandra/blob/master/src/Types/CassandraConversionHelper.cs

namespace Cassandra
{
    internal static class GuidTools
    {
        public static byte[] ToBytes(Guid value)
        {
            var bytes = value.ToByteArray();
            Array.Reverse(bytes, 0, 4);
            Array.Reverse(bytes, 4, 2);
            Array.Reverse(bytes, 6, 2);
            return bytes;
        }

        public static Guid FromBytes(byte[] value)
        {
            var bytes = (byte[])value.Clone();
            Array.Reverse(bytes, 0, 4);
            Array.Reverse(bytes, 4, 2);
            Array.Reverse(bytes, 6, 2);
            return new Guid(bytes);
        }
    }

    internal static class ConversionHelper
    {
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public static long ToUnixTime(DateTimeOffset dt)
        {
            return Convert.ToInt64(Math.Floor((dt - UnixStart).TotalMilliseconds));
        }

        public static DateTimeOffset FromUnixTime(long ms)
        {            
            return UnixStart.AddMilliseconds(ms);
        }

        public static int FromBytesToInt32(byte[] buffer, int idx)
        {
            return (int)((buffer[idx] << 24) | (buffer[idx + 1] << 16 & 0xffffff) | (buffer[idx + 2] << 8 & 0xffff) | (buffer[idx + 3] & 0xff));
        }

        public static byte[] ToBytesFromInt32(int value)
        {
            byte[] bytes = BitConverter.GetBytes((int)value);
            Array.Reverse(bytes);
            return bytes;
        }

        public static byte[] ToBytesFromInt64(long value)
        {
            byte[] bytes = BitConverter.GetBytes((long)value);
            Array.Reverse(bytes);
            return bytes;
        }

        public static ushort FromBytestToUInt16(byte[] buffer, int idx)
        {
            return (ushort)((buffer[idx] << 8) | (buffer[idx + 1] & 0xff));
        }

        public static byte[] ToBytesFromUInt16(ushort value)
        {
            byte[] bytes = BitConverter.GetBytes((ushort)value);
            Array.Reverse(bytes);
            return bytes;
        }

        public static short FromBytestToInt16(byte[] buffer, int idx)
        {
            return (short)((buffer[idx] << 8) | (buffer[idx + 1] & 0xff));
        }

        public static byte[] ToBytesFromInt16(short value)
        {
            byte[] bytes = BitConverter.GetBytes((short)value);
            Array.Reverse(bytes);
            return bytes;
        }

    }
}
