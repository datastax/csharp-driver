using System;
namespace Cassandra
{
/**
 * Top level class for (checked) exceptions thrown by the driver.
 */
    public class DriverException : Exception
    {

        public DriverException(string message)
            : base(message) { }

        public DriverException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
