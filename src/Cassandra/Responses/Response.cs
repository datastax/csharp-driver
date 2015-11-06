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
using HeaderFlag = Cassandra.FrameHeader.HeaderFlag;

namespace Cassandra.Responses
{
    internal class Response
    {
        /// <summary>
        /// Big-endian binary reader of the response frame
        /// </summary>
        protected FrameReader Reader { get; set; }

        /// <summary>
        /// Identifier of the Cassandra trace 
        /// </summary>
        protected internal Guid? TraceId { get; set; }

        internal Response(Frame frame)
        {
            if (frame == null) throw new ArgumentNullException("frame");
            if (frame.Body == null) throw new InvalidOperationException("Response body of the received frame was null");
            if (!frame.Header.Flags.HasFlag(HeaderFlag.Compression) && frame.Header.BodyLength > frame.Body.Length - frame.Body.Position)
            {
                throw new DriverInternalError(string.Format(
                    "Response body length should be contained in stream: Expected {0} but was {1} (position {2})",
                    frame.Header.BodyLength, frame.Body.Length - frame.Body.Position, frame.Body.Position));
            }

            Reader = new FrameReader(frame.Body);

            if (frame.Header.Flags.HasFlag(HeaderFlag.Tracing))
            {
                //If a response frame has the tracing flag set, the first item in its body is the trace id
                var buffer = new byte[16];
                Reader.Read(buffer, 0, 16);
                TraceId = new Guid(TypeCodec.GuidShuffle(buffer));
            }
        }
    }
}
