using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Indicates a syntax error in a query.
    /// </summary>
    public class SyntaxError : QueryValidationException
    {
        public SyntaxError(string Message) : base(Message) { }
    }
}