//
//      Copyright (C) 2012-2014 DataStax Inc.
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
        internal const byte TraceFlagValue = 0x02;

        /// <summary>
        /// Big-endian binary reader of the response frame
        /// </summary>
        protected BEBinaryReader BeBinaryReader { get; set; }

        /// <summary>
        /// Identifier of the Cassandra trace 
        /// </summary>
        protected internal Guid? TraceId { get; set; }

        internal AbstractResponse(ResponseFrame frame)
        {
            if (frame == null) throw new ArgumentNullException("frame");
            if (frame.Body == null) throw new InvalidOperationException("Response body of the received frame was null");

            BeBinaryReader = new BEBinaryReader(frame.Body);

            // If a response frame has the tracing flag set, its body contains
            // a tracing ID. The tracing ID is a [uuid] and is the first thing in
            // the frame body. 
            if ((frame.Header.Flags & TraceFlagValue) == TraceFlagValue)
            {
                var buffer = new byte[16];
                BeBinaryReader.Read(buffer, 0, 16);
                TraceId = new Guid(TypeCodec.GuidShuffle(buffer));
            }
        }
    }
}
