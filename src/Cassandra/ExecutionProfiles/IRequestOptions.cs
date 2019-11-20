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

using System;
using Cassandra.DataStax.Graph;

namespace Cassandra.ExecutionProfiles
{
    internal interface IRequestOptions
    {
        ConsistencyLevel ConsistencyLevel { get; }

        ConsistencyLevel SerialConsistencyLevel { get; }

        int ReadTimeoutMillis { get; }

        ILoadBalancingPolicy LoadBalancingPolicy { get; }

        ISpeculativeExecutionPolicy SpeculativeExecutionPolicy { get; }

        IExtendedRetryPolicy RetryPolicy { get; }

        GraphOptions GraphOptions { get; }

        //// next settings don't exist in execution profiles

        int PageSize { get; }

        ITimestampGenerator TimestampGenerator { get; }

        bool DefaultIdempotence { get; }

        int QueryAbortTimeout { get; }
        
        /// <summary>
        /// Gets the serial consistency level of the statement or the default value from the query options.
        /// </summary>
        /// <exception cref="ArgumentException" />
        ConsistencyLevel GetSerialConsistencyLevelOrDefault(IStatement statement);

        int GetQueryAbortTimeout(int amountOfQueries);
    }
}