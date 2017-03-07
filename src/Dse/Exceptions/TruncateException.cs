//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  Error during a truncation operation.
    /// </summary>
    public class TruncateException : QueryExecutionException
    {
        public TruncateException(string message) : base(message)
        {
        }
    }
}
