using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    public struct TimeUuid
    {
        private static readonly DateTimeOffset GregorianCalendarTime = new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero);
        //Reuse the random generator to avoid collisions
        private static readonly Random _randomGenerator = new Random();

        private Guid _value;

        private TimeUuid(Guid value)
        {
            _value = value;
        }

        private TimeUuid(byte[] nodeId, byte[] clockId, DateTimeOffset time)
        {
            if (nodeId == null || nodeId.Length != 6)
            {
                throw new ArgumentException("node id should contain 6 bytes");
            }
            if (clockId == null || clockId.Length != 2)
            {
                throw new ArgumentException("node id should contain 6 bytes");
            }
            var timeBytes = BitConverter.GetBytes((time - GregorianCalendarTime).Ticks);
            var buffer = new byte[16];
            //Positions 0-7 Timestamp
            Buffer.BlockCopy(timeBytes, 0, buffer, 0, 8);
            //Position 8-9 Clock
            Buffer.BlockCopy(clockId, 0, buffer, 8, 2);
            //Positions 10-15 Node
            Buffer.BlockCopy(nodeId, 0, buffer, 10, 6);

            //Version Byte: Time based
            //0001xxxx
            //turn off first 4 bits
            buffer[7] &= 0x0f; //00001111
            //turn on fifth bit
            buffer[7] |= 0x10; //00010000

            //Variant Byte: 1.0.x
            //10xxxxxx
            //turn off first 2 bits
            buffer[8] &= 0x3f; //00111111
            //turn on first bit
            buffer[8] |= 0x80; //10000000

            _value = new Guid(buffer);
        }
    }
}
