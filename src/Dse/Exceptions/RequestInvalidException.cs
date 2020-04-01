//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    /// Exception that indicates that the request is not valid.
    /// </summary>
    public class RequestInvalidException : DriverException
    {
        public RequestInvalidException(string message) : base(message)
        {

        }
    }
}
