using System;

namespace Cassandra.Data.Linq
{
    public class CqlArgumentException : ArgumentException
    {
        internal CqlArgumentException(string message)
            : base(message)
        {
        }
    }
}