//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    internal class AbstractResponse
    {
        /// <summary>
        /// Big-endian binary reader of the response frame
        /// </summary>
        protected BEBinaryReader BeBinaryReader { get; set; }
        /// <summary>
        /// Identifier of the Cassandra trace 
        /// </summary>
        protected Guid? TraceId { get; set; }

        internal AbstractResponse(ResponseFrame frame)
        {
            if (frame == null) throw new ArgumentNullException("frame");

            BeBinaryReader = new BEBinaryReader(frame.Body);

            if ((frame.Header.Flags & 0x02) == 0x02)
            {
                var buffer = new byte[16];
                BeBinaryReader.Read(buffer, 0, 16);
                TraceId = new Guid(TypeCodec.GuidShuffle(buffer));
            }
        }
    }
}