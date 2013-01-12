using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Exception thrown when the coordinator knows there is not enough replica
    /// alive to perform a query with the requested consistency level.
    /// </summary>
    public class UnavailableException : QueryExecutionException
    {
        public ConsistencyLevel ConsistencyLevel { get; private set; }
        public int Required { get; private set; }
        public int Alive { get; private set; }
        
        public UnavailableException(string Message, ConsistencyLevel ConsistencyLevel,int Required, int Alive):
            base(Message) { this.ConsistencyLevel = ConsistencyLevel; this.Required = Required; this.Alive = Alive; }
    }
}