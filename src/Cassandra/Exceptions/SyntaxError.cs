//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    /// <summary>
    ///  Indicates a syntax error in a query.
    /// </summary>
    public class SyntaxError : QueryValidationException
    {
        public SyntaxError(string message) : base(message)
        {
        }
    }
}
