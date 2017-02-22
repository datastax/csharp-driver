//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    internal class OutputPrepared : IOutput
    {
        public RowSetMetadata Metadata { get; private set; }
        public byte[] QueryId { get; private set; }
        public System.Guid? TraceId { get; internal set; }

        internal OutputPrepared(ProtocolVersion protocolVersion, FrameReader reader)
        {
            var length = reader.ReadInt16();
            QueryId = new byte[length];
            reader.Read(QueryId, 0, length);
            Metadata = new RowSetMetadata(reader, protocolVersion.SupportsPreparedPartitionKey());
        }

        public void Dispose()
        {
        }
    }
}
