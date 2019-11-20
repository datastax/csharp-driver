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
    internal class ExecutionProfileBuilder : IExecutionProfileBuilder
    {
        private int? _readTimeoutMillis;
        private ConsistencyLevel? _consistencyLevel;
        private ConsistencyLevel? _serialConsistencyLevel;
        private ILoadBalancingPolicy _loadBalancingPolicy;
        private ISpeculativeExecutionPolicy _speculativeExecutionPolicy;
        private IExtendedRetryPolicy _retryPolicy;
        
        private GraphOptions _graphOptions;
        
        public IExecutionProfileBuilder WithLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy)
        {
            _loadBalancingPolicy = loadBalancingPolicy ?? throw new ArgumentNullException(nameof(loadBalancingPolicy));
            return this;
        }

        public IExecutionProfileBuilder WithRetryPolicy(IExtendedRetryPolicy retryPolicy)
        {
            _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
            return this;
        }

        public IExecutionProfileBuilder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy speculativeExecutionPolicy)
        {
            _speculativeExecutionPolicy = speculativeExecutionPolicy ?? throw new ArgumentNullException(nameof(speculativeExecutionPolicy));
            return this;
        }

        public IExecutionProfileBuilder WithConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }
        
        public IExecutionProfileBuilder WithSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel)
        {
            _serialConsistencyLevel = serialConsistencyLevel;
            return this;
        }
        
        public IExecutionProfileBuilder WithReadTimeoutMillis(int readTimeoutMillis)
        {
            _readTimeoutMillis = readTimeoutMillis;
            return this;
        }

        /// <inheritdoc />
        public IExecutionProfileBuilder WithGraphOptions(GraphOptions graphOptions)
        {
            _graphOptions = graphOptions;
            return this;
        }

        public IExecutionProfile Build()
        {
            return new ExecutionProfile(
                _consistencyLevel,
                _serialConsistencyLevel,
                _readTimeoutMillis,
                _loadBalancingPolicy,
                _speculativeExecutionPolicy,
                _retryPolicy,
                _graphOptions);
        }
    }
}