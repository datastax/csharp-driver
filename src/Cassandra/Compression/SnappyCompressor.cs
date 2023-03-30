//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.IO;

namespace Cassandra.Compression
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
