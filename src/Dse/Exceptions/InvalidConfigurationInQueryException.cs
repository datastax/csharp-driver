//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  A specific invalid query exception that indicates that the query is invalid
    ///  because of some configuration problem. <p> This is generally throw by query
    ///  that manipulate the schema (CREATE and ALTER) when the required configuration
    ///  options are invalid.</p>
    /// </summary>
    public class InvalidConfigurationInQueryException : InvalidQueryException
    {
        public InvalidConfigurationInQueryException(string message) : base(message)
        {
        }
    }
}
