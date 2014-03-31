//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    ///  A Cassandra timeout during a query. Such an exception is returned when the
    ///  query has been tried by Cassandra but cannot be achieved with the requested
    ///  consistency level within the rpc timeout set for Cassandra.
    /// </summary>
    public abstract class QueryTimeoutException : QueryExecutionException
    {
        /// <summary>
        ///  Gets the consistency level of the operation that time outed. 
        /// </summary>
        public ConsistencyLevel ConsistencyLevel { get; private set; }

        /// <summary>
        /// Gets the number of replica that had acknowledged/responded to the operation before it time outed. 
        /// </summary>
        public int ReceivedAcknowledgements { get; private set; }

        /// <summary>
        ///  Gets the minimum number of replica acknowledgements/responses that were required to fulfill the operation. 
        /// </summary>
        public int RequiredAcknowledgements { get; private set; }

        public QueryTimeoutException(string message, ConsistencyLevel consistencyLevel, int received, int required)
            : base(message)
        {
            ConsistencyLevel = consistencyLevel;
            ReceivedAcknowledgements = received;
            RequiredAcknowledgements = required;
        }
    }
}