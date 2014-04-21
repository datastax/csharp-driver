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
    ///  Exception thrown when the coordinator knows there is not enough replica alive
    ///  to perform a query with the requested consistency level.
    /// </summary>
    public class UnavailableException : QueryExecutionException
    {
        /// <summary>
        ///  Gets the consistency level of the operation triggering this unavailable exception. 
        /// </summary>
        public ConsistencyLevel Consistency { get; private set; }

        /// <summary>
        /// Gets the number of replica acknowledgements/responses required to perform the operation (with its required consistency level). 
        /// </summary>
        public int RequiredReplicas { get; private set; }

        /// <summary>
        ///  Gets the number of replica that were known to be alive by the Cassandra coordinator node when it tried to execute the operation. 
        /// </summary>
        public int AliveReplicas { get; private set; }

        public UnavailableException(ConsistencyLevel consistency, int required, int alive) :
            base(
            string.Format("Not enough replicas available for query at consistency {0} ({1} required but only {2} alive)", consistency, required, alive)
            )
        {
            Consistency = consistency;
            RequiredReplicas = required;
            AliveReplicas = alive;
        }
    }
}