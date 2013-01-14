using System;

namespace Cassandra
{
    /// <summary>
    /// Top level class for exceptions thrown by the driver.
    /// </summary>
    public class DriverException : Exception
    {

        public DriverException(string message)
            : base(message)
        {
        }

        public DriverException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
