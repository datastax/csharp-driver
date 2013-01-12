using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// An exception indicating that a query cannot be executed because it is
    /// incorrect syntaxically, invalid, unauthorized or any other reason.
    /// </summary>
    public abstract class QueryValidationException : DriverUncheckedException
    {
        public QueryValidationException(string message)
            : base(message) { }

        public QueryValidationException(string message, Exception innerException)
            : base(message, innerException) { }

        public abstract RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries);
    }
}