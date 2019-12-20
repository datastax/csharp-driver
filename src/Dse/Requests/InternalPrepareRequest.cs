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
