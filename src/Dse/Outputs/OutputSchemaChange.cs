//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse
{
    internal class OutputSchemaChange : IOutput
    {
        public string Change;
        public string Keyspace;
        public string Table;

        public Guid? TraceId { get; private set; }

        internal OutputSchemaChange(FrameReader reader, Guid? traceId)
        {
            TraceId = traceId;
            Change = reader.ReadString();
            Keyspace = reader.ReadString();
            Table = reader.ReadString();
        }

        public void Dispose()
        {
        }
    }
}
