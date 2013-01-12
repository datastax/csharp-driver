using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Error during a truncation operation.
    /// </summary>
    public class TruncateException : QueryExecutionException
    {
        public TruncateException(string Message) : base(Message) { }
    }
}