﻿//
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

using System.Collections.Generic;
using System.IO;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class InternalPrepareRequest : IRequest
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

        public InternalPrepareRequest(string cqlQuery)
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
