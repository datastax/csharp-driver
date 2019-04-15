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

namespace Cassandra.ExecutionProfiles
{
    internal class ExecutionProfileBuilder : IExecutionProfileBuilder
    {
        private int? _readTimeoutMillis;
        private ConsistencyLevel? _consistencyLevel;
        private ConsistencyLevel? _serialConsistencyLevel;
        private ILoadBalancingPolicy _loadBalancingPolicy;
        private ISpeculativeExecutionPolicy _speculativeExecutionPolicy;
        private IExtendedRetryPolicy _retryPolicy;
        private ExecutionProfile _baseProfile;
        
        public IExecutionProfileBuilder LoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy)
        {
            _loadBalancingPolicy = loadBalancingPolicy ?? throw new ArgumentNullException(nameof(loadBalancingPolicy));
            return this;
        }

        public IExecutionProfileBuilder RetryPolicy(IExtendedRetryPolicy retryPolicy)
        {
            _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
            return this;
        }

        public IExecutionProfileBuilder SpeculativeExecutionPolicy(ISpeculativeExecutionPolicy speculativeExecutionPolicy)
        {
            _speculativeExecutionPolicy = speculativeExecutionPolicy ?? throw new ArgumentNullException(nameof(speculativeExecutionPolicy));
            return this;
        }

        public IExecutionProfileBuilder ConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }
        
        public IExecutionProfileBuilder SerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel)
        {
            _serialConsistencyLevel = serialConsistencyLevel;
            return this;
        }
        
        public IExecutionProfileBuilder ReadTimeoutMillis(int readTimeoutMillis)
        {
            _readTimeoutMillis = readTimeoutMillis;
            return this;
        }

        public IExecutionProfileBuilder DeriveFrom(ExecutionProfile baseProfile)
        {
            if (_baseProfile != null)
            {
                throw new InvalidOperationException("A base profile is already set.");
            }

            _baseProfile = baseProfile ?? throw new ArgumentNullException(nameof(baseProfile));
            return this;
        }

        public ExecutionProfile Build()
        {
            return new ExecutionProfile(
                _consistencyLevel ?? _baseProfile?.ConsistencyLevel,
                _serialConsistencyLevel ?? _baseProfile?.SerialConsistencyLevel,
                _readTimeoutMillis ?? _baseProfile?.ReadTimeoutMillis,
                _loadBalancingPolicy ?? _baseProfile?.LoadBalancingPolicy,
                _speculativeExecutionPolicy ?? _baseProfile?.SpeculativeExecutionPolicy,
                _retryPolicy ?? _baseProfile?.RetryPolicy);
        }
    }
}