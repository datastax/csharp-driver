using System.IO;
namespace Cassandra
{
    internal class ResponseFrame
    {
        public const byte ProtocolV1ResponseVersionByte = 0x81;
        public const byte ProtocolV2ResponseVersionByte = 0x82;

        /// <summary>
        /// The 8 byte protocol header
        /// </summary>
        public FrameHeader Header { get; set; }

        /// <summary>
        /// A stream representing the frame body
        /// </summary>
        public Stream Body { get; set; }

        public ResponseFrame(FrameHeader header, Stream body)
        {
            Header = header;
            Body = body;
            //Start at the first byte
            Body.Position = 0;
        }
    }
}