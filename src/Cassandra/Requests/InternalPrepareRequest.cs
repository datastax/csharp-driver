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

using System;
using System.Collections.Generic;
using System.IO;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class InternalPrepareRequest : IRequest
    {
        public const byte OpCode = 0x09;
        private readonly IDictionary<string, byte[]> _payload;
        private readonly FrameHeader.HeaderFlag _headerFlags;
        private readonly PrepareFlags _prepareFlags = 0;

        [Flags]
        internal enum PrepareFlags
        {
            WithKeyspace = 0x01
        }

        /// <summary>
        /// Gets the keyspace for the query, only defined when keyspace is different than the current keyspace.
        /// </summary>
        public string Keyspace { get; }

        /// <summary>
        /// The CQL string to be prepared
        /// </summary>
        public string Query { get; set; }

        public InternalPrepareRequest(string cqlQuery, string keyspace = null, IDictionary<string, byte[]> payload = null)
        {
            Query = cqlQuery;
            Keyspace = keyspace;
            _payload = payload;
            if (payload != null)
            {
                _headerFlags |= FrameHeader.HeaderFlag.CustomPayload;
            }

            if (keyspace != null)
            {
                _prepareFlags |= PrepareFlags.WithKeyspace;
            }
        }

        public int WriteFrame(short streamId, MemoryStream stream, ISerializer serializer)
        {
            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader((byte)_headerFlags, streamId, OpCode);
            var protocolVersion = serializer.ProtocolVersion;

            if (_payload != null)
            {
                wb.WriteBytesMap(_payload);
            }

            wb.WriteLongString(Query);

            if (protocolVersion.SupportsKeyspaceInRequest())
            {
                wb.WriteInt32((int) _prepareFlags);
                if (Keyspace != null)
                {
                    wb.WriteString(Keyspace);
                }
            }

            return wb.Close();
        }
    }
}
