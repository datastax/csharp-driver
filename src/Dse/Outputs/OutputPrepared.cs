//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

// ReSharper disable once CheckNamespace
namespace Dse
{
    internal class OutputPrepared : IOutput
    {
        public RowSetMetadata Metadata { get; }

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

            Metadata = new RowSetMetadata(reader, protocolVersion.SupportsPreparedPartitionKey());
        }
        
        // for testing
        internal OutputPrepared(byte[] queryId, RowSetMetadata rowSetMetadata)
        {
            QueryId = queryId;
            Metadata = rowSetMetadata;
        }

        public void Dispose()
        {
        }
    }
}
