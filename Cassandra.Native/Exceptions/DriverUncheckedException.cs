using System;
namespace Cassandra
{
    /// <summary>
    /// Top level class for unchecked exceptions thrown by the driver.
    /// </summary> 
    public class DriverUncheckedException : Exception
    {

        public DriverUncheckedException(string message)
            : base(message) { }

        public DriverUncheckedException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}

