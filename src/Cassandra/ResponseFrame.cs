namespace Cassandra
{
    internal class ResponseFrame
    {
        public const byte ProtocolV1ResponseVersionByte = 0x81;
        public const byte ProtocolV2ResponseVersionByte = 0x82;

        public FrameHeader FrameHeader;
        public IProtoBuf RawStream;
    }
}