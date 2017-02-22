//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra
{
    /// <summary>
    ///  An unexpected error happened internally. This should never be raise and
    ///  indicates an unexpected behavior (either in the driver or in Cassandra).
    /// </summary>
    public class DriverInternalError : Exception
    {
        public DriverInternalError(string message)
            : base(message)
        {
        }

        public DriverInternalError(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
