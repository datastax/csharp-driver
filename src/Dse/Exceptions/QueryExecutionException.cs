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
    ///  Exception related to the execution of a query. This correspond to the
    ///  exception that Cassandra throw when a (valid) query cannot be executed
    ///  (TimeoutException, UnavailableException, ...).
    /// </summary>
    public abstract class QueryExecutionException : QueryValidationException
    {
        public QueryExecutionException(string message)
            : base(message)
        {
        }

        public QueryExecutionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
