//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra
{
    /// <summary>
    ///  Exception thrown if a query trace cannot be retrieved.
    /// </summary>
    public class TraceRetrievalException : DriverException
    {
        public TraceRetrievalException(string message)
            : base(message)
        {
        }

        public TraceRetrievalException(string message, Exception cause)
            : base(message, cause)
        {
        }
    }
}
