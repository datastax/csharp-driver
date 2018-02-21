//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  A server timeout during a read query.
    /// </summary>
    public class ReadTimeoutException : QueryTimeoutException
    {
        public bool WasDataRetrieved { get; private set; }

        public ReadTimeoutException(ConsistencyLevel consistency, int received, int required, bool dataPresent) :
            base("Server timeout during read query at consistency" +
                 $" {consistency} ({FormatDetails(received, required, dataPresent)})",
                 consistency,
                 received,
                 required)
        {
            WasDataRetrieved = dataPresent;
        }

        private static string FormatDetails(int received, int required, bool dataPresent)
        {
            if (received < required)
            {
                return $"{received} replica(s) responded over {required} required";
            }

            if (!dataPresent)
            {
                return "the replica queried for data didn't respond";
            }
            return "timeout while waiting for repair of inconsistent replica";
        }
    }
}
