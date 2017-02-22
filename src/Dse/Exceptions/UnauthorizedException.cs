//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  Indicates that a query cannot be performed due to the authorisation restrictions of the logged user.
    /// </summary>
    public class UnauthorizedException : QueryValidationException
    {
        public UnauthorizedException(string message) : base(message)
        {
        }
    }
}
