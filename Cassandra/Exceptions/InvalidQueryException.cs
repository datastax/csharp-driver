using System;

namespace Cassandra
{
    /// <summary>
    ///  Indicates a syntactically correct but invalid query.
    /// </summary>
    public class InvalidQueryException : QueryValidationException
    {
        public InvalidQueryException(string message)
            : base(message)
        {
        }

        public InvalidQueryException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
