//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.IO;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class AuthResponseRequest : IRequest
    {
        public const byte OpCode = 0x0F;
        private readonly byte[] _token;

        public AuthResponseRequest(byte[] token)
        {
            _token = token;
        }

        public int WriteFrame(short streamId, MemoryStream stream, Serializer serializer)
        {
            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader(0x00, streamId, OpCode);
            wb.WriteBytes(_token);
            return wb.Close();
        }
    }
}
