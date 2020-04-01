//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.IO;

namespace Dse.Compression
{
    internal class SnappyCompressor : IFrameCompressor
    {
        public Stream Decompress(Stream stream)
        {
            var buffer = Utils.ReadAllBytes(stream, 0);
            var outputBuffer = Snappy.SnappyDecompressor.Uncompress(buffer, 0, buffer.Length);
            return new MemoryStream(outputBuffer, 0, outputBuffer.Length, false, true);
        }
    }
}
