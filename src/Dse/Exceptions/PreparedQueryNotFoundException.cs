//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    public class PreparedQueryNotFoundException : QueryValidationException
    {
        public byte[] UnknownId { get; private set; }

        public PreparedQueryNotFoundException(string message, byte[] unknownId) 
            : base(message)
        {
            UnknownId = unknownId;
        }
    }
}
