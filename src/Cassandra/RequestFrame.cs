using System.IO;

namespace Cassandra
{
    internal class RequestFrame
    {
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