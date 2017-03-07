//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.IO;
using Dse.Serialization;

namespace Dse.Requests
{
    internal class PrepareRequest : IRequest
    {
        public const byte OpCode = 0x09;
        private IDictionary<string, byte[]> _payload;
        private FrameHeader.HeaderFlag _headerFlags;
        /// <summary>
        /// The CQL string to be prepared
        /// </summary>
        public string Query { get; set; }

        public IDictionary<string, byte[]> Payload
        {
            get { return _payload; }
            set
            {
                if (value != null)
                {
                    _headerFlags |= FrameHeader.HeaderFlag.CustomPayload;
                }
                _payload = value;
            }
        }

        public PrepareRequest(string cqlQuery)
        {
            Query = cqlQuery;
        }

        public int WriteFrame(short streamId, MemoryStream stream, Serializer serializer)
        {
            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader((byte)_headerFlags, streamId, OpCode);
            if (Payload != null)
            {
                wb.WriteBytesMap(Payload);
            }
            wb.WriteLongString(Query);
            return wb.Close();
        }
    }
}
