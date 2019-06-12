//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    public class OverloadedException : QueryValidationException
    {
        public OverloadedException(string message) : base(message)
        {
        }
    }
}