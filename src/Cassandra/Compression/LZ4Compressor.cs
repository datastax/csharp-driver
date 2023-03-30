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

using System;
using System.IO;

using K4os.Compression.LZ4;

namespace Cassandra.Compression
{
    internal class LZ4Compressor : IFrameCompressor
    {
        public Stream Decompress(Stream stream)
        {
            var buffer = Utils.ReadAllBytes(stream, 0);
            if (buffer.Length < 4)
            {
                throw new DriverInternalError("Corrupt literal length");
            }

            var outputLengthBytes = new byte[4];
            Buffer.BlockCopy(buffer, 0, outputLengthBytes, 0, 4);
            Array.Reverse(outputLengthBytes);
            var outputLength = BitConverter.ToInt32(outputLengthBytes, 0);
            var outputBuffer = new byte[outputLength];
            if (outputLength != 0)
            {
                var uncompressedSize = LZ4Codec.Decode(buffer, 4, buffer.Length - 4, outputBuffer, 0, outputLength);
                if (uncompressedSize < 0)
                {
                    throw new DriverInternalError("LZ4 decoded buffer has a larger size than the expected uncompressed size");
                }

                if (outputLength != uncompressedSize)
                {
                    throw new DriverInternalError(string.Format("Recorded length is {0} bytes but actual length after decompression is {1} bytes ",
                        outputLength,
                        uncompressedSize));
                }
            }

            return new MemoryStream(outputBuffer, 0, outputLength, false, true);
        }
    }
}
