//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra
{
    internal class OutputVoid : IOutput
    {
        public Guid? TraceId { get; private set; }

        public OutputVoid(Guid? traceId)
        {
            TraceId = traceId;
        }

        public void Dispose()
        {
        }
    }
}
