//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    public class ServerErrorException : QueryValidationException
    {
        public ServerErrorException(string message) : base(message)
        {
        }
    }
}