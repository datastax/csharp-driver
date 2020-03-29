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

namespace Cassandra
{
    /// <summary>
    /// Defines the methods for frame compression and decompression
    /// </summary>
    public interface IFrameCompressor
    {
        /// <summary>
        /// Creates and returns stream (clear text) using the provided compressed <c>stream</c> as input.
        /// </summary>
        Stream Decompress(Stream stream);
    }
}
