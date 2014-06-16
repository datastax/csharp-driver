using System.IO;

namespace Cassandra
{
    internal class RequestFrame
    {
        public const byte ProtocolV1RequestVersionByte = 0x01;
        public const byte ProtocolV2RequestVersionByte = 0x02;

        /// <summary>
        /// Gets or sets the underlying stream that contains the full frame
        /// </summary>
        public Stream Stream { get; protected set; }

        public RequestFrame(Stream stream)
        {
            this.Stream = stream;
        }
    }
}