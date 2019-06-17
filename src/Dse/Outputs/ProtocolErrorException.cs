//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse
{
    public class ProtocolErrorException : QueryValidationException
    {
        public ProtocolErrorException(string message) : base(message)
        {
        }
    }
}