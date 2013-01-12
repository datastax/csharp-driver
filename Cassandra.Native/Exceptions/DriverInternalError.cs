using System;
namespace Cassandra
{
    /// <summary>
    /// An unexpected error happened internally.
    /// This should never be raise and indicates a bug (either in the driver or in
    /// Cassandra).
    /// </summary>
    public class DriverInternalError : Exception
    {

        public DriverInternalError(string message)
            : base(message) { }

        public DriverInternalError(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
