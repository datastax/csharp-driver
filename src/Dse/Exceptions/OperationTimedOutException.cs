//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Net;

using Dse.Connections;

// ReSharper disable once CheckNamespace
namespace Dse
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