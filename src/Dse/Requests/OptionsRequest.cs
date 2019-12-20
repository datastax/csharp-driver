//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.IO;
using Dse.Serialization;

namespace Dse.Requests
{
    internal class OptionsRequest : IRequest
    {
        public const byte OpCode = 0x05;

        public int WriteFrame(short streamId, MemoryStream stream, ISerializer serializer)
        {
            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader(0x00, streamId, OpCode);
            return wb.Close();
        }
    }
}
