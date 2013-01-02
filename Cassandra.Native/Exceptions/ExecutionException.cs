using System;
using System.Collections.Generic;
using System.Text;
using System.Net;

namespace Cassandra
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
