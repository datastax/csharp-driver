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
    internal class ExecutionProfile : IExecutionProfile
    {
        internal ExecutionProfile(
            ConsistencyLevel? consistencyLevel, 
            ConsistencyLevel? serialConsistencyLevel, 
            int? readTimeoutMillis, 
            ILoadBalancingPolicy loadBalancingPolicy, 
            ISpeculativeExecutionPolicy speculativeExecutionPolicy, 
            IExtendedRetryPolicy retryPolicy,
            GraphOptions graphOptions)
        {
            Initialize(
                consistencyLevel, 
                serialConsistencyLevel, 
                readTimeoutMillis, 
                loadBalancingPolicy, 
                speculativeExecutionPolicy, 
                retryPolicy,
                graphOptions);
        }

        internal ExecutionProfile(IExecutionProfile baseProfile, IExecutionProfile profile)
        {
            if (baseProfile == null)
            {
                throw new ArgumentNullException(nameof(baseProfile));
            }
            
            if (profile == null)
            {
                throw new ArgumentNullException(nameof(profile));
            }
            
            Initialize(
                profile.ConsistencyLevel ?? baseProfile.ConsistencyLevel, 
                profile.SerialConsistencyLevel ?? baseProfile.SerialConsistencyLevel, 
                profile.ReadTimeoutMillis ?? baseProfile.ReadTimeoutMillis, 
                profile.LoadBalancingPolicy ?? baseProfile.LoadBalancingPolicy, 
                profile.SpeculativeExecutionPolicy ?? baseProfile.SpeculativeExecutionPolicy, 
                profile.RetryPolicy ?? baseProfile.RetryPolicy,
                profile.GraphOptions ?? baseProfile.GraphOptions);
        }

        internal ExecutionProfile(IRequestOptions requestOptions)
        {
            if (requestOptions == null)
            {
                throw new ArgumentNullException(nameof(requestOptions));
            }

            Initialize(
                requestOptions.ConsistencyLevel, 
                requestOptions.SerialConsistencyLevel, 
                requestOptions.ReadTimeoutMillis, 
                requestOptions.LoadBalancingPolicy, 
                requestOptions.SpeculativeExecutionPolicy, 
                requestOptions.RetryPolicy,
                requestOptions.GraphOptions);
        }

        public ConsistencyLevel? ConsistencyLevel { get; private set; }

        public ConsistencyLevel? SerialConsistencyLevel { get; private set; }

        public int? ReadTimeoutMillis { get; private set; }

        public ILoadBalancingPolicy LoadBalancingPolicy { get; private set; }

        public ISpeculativeExecutionPolicy SpeculativeExecutionPolicy { get; private set; }

        public IExtendedRetryPolicy RetryPolicy { get; private set; }

        public GraphOptions GraphOptions { get; private set; }

        private void Initialize(
            ConsistencyLevel? consistencyLevel,
            ConsistencyLevel? serialConsistencyLevel,
            int? readTimeoutMillis,
            ILoadBalancingPolicy loadBalancingPolicy,
            ISpeculativeExecutionPolicy speculativeExecutionPolicy,
            IExtendedRetryPolicy retryPolicy,
            GraphOptions graphOptions)
        {
            ConsistencyLevel = consistencyLevel;
            SerialConsistencyLevel = serialConsistencyLevel;
            ReadTimeoutMillis = readTimeoutMillis;
            LoadBalancingPolicy = loadBalancingPolicy;
            SpeculativeExecutionPolicy = speculativeExecutionPolicy;
            RetryPolicy = retryPolicy;
            GraphOptions = graphOptions;
        }
    }
}