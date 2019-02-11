using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// Specifies a User defined function execution failure.
    /// </summary>
    public class FunctionFailureException : DriverException
    {
        /// <summary>
        /// Keyspace where the function is defined
        /// </summary>
        public string Keyspace { get; set; }

        /// <summary>
        /// Name of the function
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Name types of the arguments
        /// </summary>
        public string[] ArgumentTypes { get; set; }

        public FunctionFailureException(string message) : base(message)
        {
        }

        public FunctionFailureException(string message, Exception innerException) : base(message, innerException)
        {
        }

#if NET45
        protected FunctionFailureException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
#endif
    }
}
