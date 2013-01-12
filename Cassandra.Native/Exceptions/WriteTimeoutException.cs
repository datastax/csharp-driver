using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// A Cassandra timeout during a write query.
    /// </summary>
    public class WriteTimeoutException : QueryTimeoutException
    {
        public string WriteType { get; private set; }
        public WriteTimeoutException(string Message, ConsistencyLevel ConsistencyLevel, int Received, int BlockFor, string WriteType) :
            base(Message, ConsistencyLevel, Received, BlockFor) { this.WriteType = WriteType; }
    }
}