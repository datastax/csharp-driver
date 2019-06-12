//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.IO;
using Dse.Serialization;

namespace Dse.Requests
{
    internal class CredentialsRequest : IRequest
    {
        public const byte OpCode = 0x04;
        private readonly IDictionary<string, string> _credentials;

        public CredentialsRequest(IDictionary<string, string> credentials)
        {
            _credentials = credentials;
        }

        public int WriteFrame(short streamId, MemoryStream stream, Serializer serializer)
        {
            if (serializer.ProtocolVersion != ProtocolVersion.V1)
            {
                throw new NotSupportedException("Credentials request is only supported in C* = 1.2.x");
            }

            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader(0x00, streamId, OpCode);
            wb.WriteUInt16((ushort) _credentials.Count);
            foreach (var kv in _credentials)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.Close();
        }
    }
}
