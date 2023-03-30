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

using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal class PrepareRequest : BaseRequest
    {
        public const byte PrepareOpCode = 0x09;

        private readonly PrepareFlags? _prepareFlags = PrepareFlags.None;

        [Flags]
        internal enum PrepareFlags
        {
            None = 0,
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
        
        /// <inheritdoc />
        public override ResultMetadata ResultMetadata => null;

        public PrepareRequest(
            ISerializer serializer, string cqlQuery, string keyspace, IDictionary<string, byte[]> payload)
            : base(serializer, false, payload)
        {
            Query = cqlQuery;
            Keyspace = keyspace;

            if (!serializer.ProtocolVersion.SupportsKeyspaceInRequest())
            {
                // if the keyspace parameter is not supported then prepare flags aren't either
                _prepareFlags = null;
                
                // and also no other optional parameter is supported
                return;
            }
            
            if (keyspace != null)
            {
                _prepareFlags |= PrepareFlags.WithKeyspace;
            }
        }

        protected override byte OpCode => PrepareRequest.PrepareOpCode;

        protected override void WriteBody(FrameWriter wb)
        {
            wb.WriteLongString(Query);

            if (_prepareFlags != null)
            {
                wb.WriteInt32((int)_prepareFlags);
                if (_prepareFlags.Value.HasFlag(PrepareFlags.WithKeyspace))
                {
                    wb.WriteString(Keyspace);
                }
            }
        }
    }
}