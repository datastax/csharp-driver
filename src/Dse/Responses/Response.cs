//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Serialization;
using HeaderFlag = Dse.FrameHeader.HeaderFlag;

namespace Dse.Responses
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

            Reader = new FrameReader(frame.Body, frame.Serializer);

            if (frame.Header.Flags.HasFlag(HeaderFlag.Tracing))
            {
                //If a response frame has the tracing flag set, the first item in its body is the trace id
                var buffer = new byte[16];
                Reader.Read(buffer, 0, 16);
                TraceId = new Guid(TypeSerializer.GuidShuffle(buffer));
            }
        }
    }
}
