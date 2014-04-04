using System.IO;

namespace Cassandra
{
    internal struct RequestFrame
    {
        public const int VersionIdx = 0;
        public const int FlagsIdx = 1;
        public const int StreamIdIdx = 2;
        public const int OpcodeIdIdx = 3;
        public const int LenIdx = 4;
        public const int BodyIdx = 8;

        public const byte ProtocolV1RequestVersionByte = 0x01;
        public const byte ProtocolV2RequestVersionByte = 0x02;
        public MemoryTributary Buffer;
    }
}