using System;

namespace Cassandra
{
    /// <summary>
    ///  An exception indicating that a query cannot be executed because it is incorrect syntaxically, invalid, unauthorized or any other reason.
    /// </summary>
    public abstract class QueryValidationException : DriverException
    {
        public QueryValidationException(string message)
            : base(message)
        {
        }

        public QueryValidationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

    }
}