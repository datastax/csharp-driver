//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;

using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal abstract class BaseRequest : IRequest
    {
        private readonly HeaderFlags _headerFlags;
        
        /// <summary>
        /// Constructor that forces a specific serializer, i.e., protocol version.
        /// </summary>
        protected BaseRequest(ISerializer serializer, bool tracingEnabled, IDictionary<string, byte[]> payload)
            : this(tracingEnabled, payload)
        {
            Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }
        
        /// <summary>
        /// Constructor that doesn't specify the serializer, will use the connection one.
        /// </summary>
        protected BaseRequest(bool tracingEnabled, IDictionary<string, byte[]> payload)
        {
            TracingEnabled = tracingEnabled;
            Payload = payload;

            _headerFlags = HeaderFlags.None;

            if (tracingEnabled)
            {
                _headerFlags |= HeaderFlags.Tracing;
            }

            if (payload != null)
            {
                _headerFlags |= HeaderFlags.CustomPayload;
            }
        }

        private ISerializer Serializer { get; }

        public bool TracingEnabled { get; }

        public IDictionary<string, byte[]> Payload { get; }

        protected abstract byte OpCode { get; }

        /// <inheritdoc />
        public abstract ResultMetadata ResultMetadata { get; }

        protected abstract void WriteBody(FrameWriter wb);

        public int WriteFrame(short streamId, MemoryStream stream, ISerializer connectionSerializer)
        {
            var wb = new FrameWriter(stream, Serializer ?? connectionSerializer);

            WriteFrameHeader(wb, streamId);

            if (Payload != null)
            {
                //A custom payload for this request
                wb.WriteBytesMap(Payload);
            }

            WriteBody(wb);

            return wb.Close();
        }

        private void WriteFrameHeader(FrameWriter writer, short streamId)
        {
            var flags = _headerFlags;

            if (writer.Serializer.ProtocolVersion.IsBeta())
            {
                flags |= HeaderFlags.UseBeta;
            }

            writer.WriteFrameHeader((byte)flags, streamId, OpCode);
        }
    }
}