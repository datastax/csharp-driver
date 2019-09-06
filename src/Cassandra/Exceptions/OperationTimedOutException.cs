using System;
using System.Net;
using Cassandra.Connections;

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// Exception that thrown on a client-side timeout, when the client didn't hear back from the server within <see cref="SocketOptions.ReadTimeoutMillis"/>.
    /// </summary>
    public class OperationTimedOutException : DriverException
    {
        public OperationTimedOutException(IPEndPoint address, int timeout) :
            base($"The host {address} did not reply before timeout {timeout}ms")
        {
        }

        internal OperationTimedOutException(IConnectionEndPoint endPoint, int timeout) :
            base($"The host {endPoint} did not reply before timeout {timeout}ms")
        {
        }
    }
}