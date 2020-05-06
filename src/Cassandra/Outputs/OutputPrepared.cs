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

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    internal class OutputPrepared : IOutput
    {
        public RowSetMetadata VariablesRowsMetadata { get; }

        public RowSetMetadata ResultRowsMetadata { get; }

        public byte[] QueryId { get; }

        public byte[] ResultMetadataId { get; }

        public System.Guid? TraceId { get; internal set; }

        internal OutputPrepared(ProtocolVersion protocolVersion, FrameReader reader)
        {
            QueryId = reader.ReadShortBytes();

            if (protocolVersion.SupportsResultMetadataId())
            {
                ResultMetadataId = reader.ReadShortBytes();
            }

            VariablesRowsMetadata = new RowSetMetadata(reader, protocolVersion.SupportsPreparedPartitionKey());
            ResultRowsMetadata = new RowSetMetadata(reader, false);
        }
        
        // for testing
        internal OutputPrepared(byte[] queryId, RowSetMetadata rowSetVariablesRowsMetadata, RowSetMetadata resultRowsMetadata)
        {
            QueryId = queryId;
            VariablesRowsMetadata = rowSetVariablesRowsMetadata;
            ResultRowsMetadata = resultRowsMetadata;
        }

        public void Dispose()
        {
        }
    }
}
