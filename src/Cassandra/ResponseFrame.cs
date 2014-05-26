using System.IO;
namespace Cassandra
{
    internal class ResponseFrame
    {
        public const byte ProtocolV1ResponseVersionByte = 0x81;
        public const byte ProtocolV2ResponseVersionByte = 0x82;

        public FrameHeader FrameHeader { get; set; }

        public Stream RawStream { get; set; }

        public ResponseFrame(FrameHeader header, Stream body)
        {
            FrameHeader = header;
            //TODO: RENAME
            RawStream = body;
        }
    }
}