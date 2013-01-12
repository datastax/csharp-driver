using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Indicates that a query cannot be performed due to the authorisation
    /// restrictions of the logged user.
    /// </summary>
    public class UnauthorizedException : QueryValidationException
    {
        public UnauthorizedException(string Message) : base(Message) { }
    }
}
