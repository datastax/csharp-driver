//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    public class ServerErrorException : QueryValidationException
    {
        public ServerErrorException(string message) : base(message)
        {
        }
    }
}