//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Net;

namespace Dse
{
    public class ExecutionException : DriverException
    {
        public Dictionary<IPAddress, Exception> InnerInnerExceptions { get; private set; }

        public ExecutionException(string message, Exception innerException = null, Dictionary<IPAddress, Exception> innerInnerExceptions = null)
            : base(message, innerException)
        {
            InnerInnerExceptions = innerInnerExceptions;
        }
    }
}
