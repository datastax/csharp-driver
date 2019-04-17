// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System;
using Cassandra.ExecutionProfiles;

namespace Cassandra
{
    public class ExecutionProfile
    {
        internal ExecutionProfile(
            ConsistencyLevel? consistencyLevel, 
            ConsistencyLevel? serialConsistencyLevel, 
            int? readTimeoutMillis, 
            ILoadBalancingPolicy loadBalancingPolicy, 
            ISpeculativeExecutionPolicy speculativeExecutionPolicy, 
            IExtendedRetryPolicy retryPolicy)
        {
            ConsistencyLevel = consistencyLevel;
            SerialConsistencyLevel = serialConsistencyLevel;
            ReadTimeoutMillis = readTimeoutMillis;
            LoadBalancingPolicy = loadBalancingPolicy;
            SpeculativeExecutionPolicy = speculativeExecutionPolicy;
            RetryPolicy = retryPolicy;
        }

        internal ExecutionProfile(ExecutionProfile baseProfile, ExecutionProfile profile)
        {
            if (baseProfile == null)
            {
                throw new ArgumentNullException(nameof(baseProfile));
            }
            
            if (profile == null)
            {
                throw new ArgumentNullException(nameof(profile));
            }

            ConsistencyLevel = profile.ConsistencyLevel ?? baseProfile.ConsistencyLevel;
            SerialConsistencyLevel = profile.SerialConsistencyLevel ?? baseProfile.SerialConsistencyLevel;
            ReadTimeoutMillis = profile.ReadTimeoutMillis ?? baseProfile.ReadTimeoutMillis;
            LoadBalancingPolicy = profile.LoadBalancingPolicy ?? baseProfile.LoadBalancingPolicy;
            SpeculativeExecutionPolicy = profile.SpeculativeExecutionPolicy ?? baseProfile.SpeculativeExecutionPolicy;
            RetryPolicy = profile.RetryPolicy ?? baseProfile.RetryPolicy;
        }

        public ConsistencyLevel? ConsistencyLevel { get; }

        public ConsistencyLevel? SerialConsistencyLevel { get; }

        public int? ReadTimeoutMillis { get; }

        public ILoadBalancingPolicy LoadBalancingPolicy { get; }

        public ISpeculativeExecutionPolicy SpeculativeExecutionPolicy { get; }

        public IExtendedRetryPolicy RetryPolicy { get; }

        public static IExecutionProfileBuilder Builder()
        {
            return new ExecutionProfileBuilder();
        }
    }
}