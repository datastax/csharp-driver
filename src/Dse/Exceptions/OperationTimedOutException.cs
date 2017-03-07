//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Net;

// ReSharper disable once CheckNamespace
namespace Dse
{
    /// <summary>
    /// Exception that thrown on a client-side timeout, when the client didn't hear back from the server within <see cref="SocketOptions.ReadTimeoutMillis"/>.
    /// </summary>
    public class OperationTimedOutException : DriverException
    {
        public OperationTimedOutException(IPEndPoint address, int timeout) : 
            base(String.Format("The host {0} did not reply before timeout {1}ms", address, timeout))
        {
            
        }
    }
}
