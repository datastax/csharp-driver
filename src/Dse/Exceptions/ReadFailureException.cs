//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    ///  A Cassandra failure (non-timeout) during a read query.
    /// </summary>
    public class ReadFailureException : QueryExecutionException
    {
        public bool WasDataRetrieved { get; private set; }
        /// <summary>
        ///  Gets the consistency level of the operation
        /// </summary>
        public ConsistencyLevel ConsistencyLevel { get; private set; }

        /// <summary>
        /// Gets the number of replica that had acknowledged/responded to the operation
        /// </summary>
        public int ReceivedAcknowledgements { get; private set; }

        /// <summary>
        ///  Gets the minimum number of replica acknowledgements/responses that were required to fulfill the operation. 
        /// </summary>
        public int RequiredAcknowledgements { get; private set; }

        /// <summary>
        /// Gets the number of nodes that experienced a failure while executing the request.
        /// </summary>
        public int Failures { get; private set; }

        public ReadFailureException(ConsistencyLevel consistency, int received, int required, bool dataPresent, int failures) :
                                        base(string.Format("Cassandra failure during read query at consistency {0} ({1})",
                                                      consistency, FormatDetails(received, required, dataPresent)))
        {
            ConsistencyLevel = consistency;
            ReceivedAcknowledgements = received;
            RequiredAcknowledgements = required;
            WasDataRetrieved = dataPresent;
            Failures = failures;
        }

        private static string FormatDetails(int received, int required, bool dataPresent)
        {
            if (received < required)
            {
                return string.Format("{0} replica(s) responded over {1} required", received, required);
            }
            if (!dataPresent)
            {
                return string.Format("the replica queried for data didn't respond");
            }
            return string.Format("failure while waiting for repair of inconsistent replica");
        }
    }
}
