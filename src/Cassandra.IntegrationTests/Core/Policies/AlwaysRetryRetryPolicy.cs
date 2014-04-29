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

namespace Cassandra.IntegrationTests.Core.Policies
{
    public class AlwaysRetryRetryPolicy : IRetryPolicy
    {
        public static readonly AlwaysRetryRetryPolicy Instance = new AlwaysRetryRetryPolicy();

        private AlwaysRetryRetryPolicy()
        {
        }

        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                           int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }
    }
}