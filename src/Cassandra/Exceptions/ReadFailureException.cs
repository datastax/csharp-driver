//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.Text;

namespace Cassandra
{
    /// <summary>
    ///  A Server failure (non-timeout) during a read query.
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
                                        base(FormatMessage(consistency, received, required, dataPresent, failures))
        {
            ConsistencyLevel = consistency;
            ReceivedAcknowledgements = received;
            RequiredAcknowledgements = required;
            WasDataRetrieved = dataPresent;
            Failures = failures;
        }

        private static string FormatMessage(ConsistencyLevel consistency, int received, int required, bool dataPresent,
                                            int failures)
        {
            var message = new StringBuilder(150);
            message.Append("Server failure during read query at consistency ")
                   .Append(consistency).Append(" (");

            if (received < required)
            {
                message.Append(required).Append(" response(s) were required but only ")
                       .Append(received).Append(" replica(s) responded, ")
                       .Append(failures).Append(" failed");
            }
            else if (!dataPresent)
            {
                message.Append("the replica queried for data didn't respond");
            }
            else
            {
                message.Append("failure while waiting for repair of inconsistent replica");
            }

            return message.Append(")").ToString();
        }
    }
}
