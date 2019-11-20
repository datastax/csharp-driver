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
using System.Threading;
using Cassandra.DataStax.Graph;

namespace Cassandra.ExecutionProfiles
{
    /// <summary>
    /// This class differs from <see cref="ExecutionProfile"/> in the sense that there are no nullable properties and it contains
    /// some additional properties that do not exist in execution profiles but are set per request (and usually settable in Statements).
    /// Every property is guaranteed to have a value, either from a user provided profile or from the cluster config.
    /// </summary>
    internal class RequestOptions : IRequestOptions
    {
        private readonly IExecutionProfile _profile;
        private readonly IExecutionProfile _defaultProfile;
        private readonly Policies _policies;
        private readonly SocketOptions _socketOptions;
        private readonly QueryOptions _queryOptions;
        private readonly ClientOptions _clientOptions;

        private readonly GraphOptions _graphOptions;

        /// <summary>
        /// Builds a request options object without any null settings.
        /// </summary>
        /// <param name="profile">Execution profile that was mapped into this instance. Can be null if it's the default profile.</param>
        /// <param name="defaultProfile">Default execution profile. Can be null.</param>
        /// <param name="policies">Must not be null and the inner policy settings must not be null either.</param>
        /// <param name="socketOptions">Must not be null.</param>
        /// <param name="queryOptions">Must not be null.</param>
        /// <param name="clientOptions">Must not be null.</param>
        public RequestOptions(
            IExecutionProfile profile, 
            IExecutionProfile defaultProfile, 
            Policies policies, 
            SocketOptions socketOptions, 
            QueryOptions queryOptions, 
            ClientOptions clientOptions)
        {
            _profile = profile;
            _defaultProfile = defaultProfile;
            _policies = policies ?? throw new ArgumentNullException(nameof(policies));
            _socketOptions = socketOptions ?? throw new ArgumentNullException(nameof(socketOptions));
            _queryOptions = queryOptions ?? throw new ArgumentNullException(nameof(queryOptions));
            _clientOptions = clientOptions ?? throw new ArgumentNullException(nameof(clientOptions));
            
            if (profile?.LoadBalancingPolicy == null && policies.LoadBalancingPolicy == null)
            {
                throw new ArgumentNullException(nameof(policies.LoadBalancingPolicy));
            }

            if (profile?.SpeculativeExecutionPolicy == null && policies.SpeculativeExecutionPolicy == null)
            {
                throw new ArgumentNullException(nameof(policies.SpeculativeExecutionPolicy));
            }
            
            if (profile?.RetryPolicy == null && policies.RetryPolicy == null)
            {
                throw new ArgumentNullException(nameof(policies.ExtendedRetryPolicy));
            }
        }
        
        /// <summary>
        /// Builds a request options object without any null settings.
        /// </summary>
        /// <param name="profile">Execution profile that was mapped into this instance. Can be null if it's the default profile.</param>
        /// <param name="defaultProfile">Default execution profile. Can be null.</param>
        /// <param name="policies">Must not be null and the inner policy settings must not be null either.</param>
        /// <param name="socketOptions">Must not be null.</param>
        /// <param name="queryOptions">Must not be null.</param>
        /// <param name="clientOptions">Must not be null.</param>
        /// <param name="graphOptions">Must not be null.</param>
        public RequestOptions(
            IExecutionProfile profile, 
            IExecutionProfile defaultProfile, 
            Policies policies, 
            SocketOptions socketOptions, 
            QueryOptions queryOptions, 
            ClientOptions clientOptions,
            GraphOptions graphOptions) : this(profile, defaultProfile, policies, socketOptions, queryOptions, clientOptions)
        {
            _graphOptions = graphOptions ?? throw new ArgumentNullException(nameof(graphOptions));
        }

        public ConsistencyLevel ConsistencyLevel => _profile?.ConsistencyLevel ?? _defaultProfile?.ConsistencyLevel ?? _queryOptions.GetConsistencyLevel();

        public ConsistencyLevel SerialConsistencyLevel => _profile?.SerialConsistencyLevel ?? _defaultProfile?.SerialConsistencyLevel ?? _queryOptions.GetSerialConsistencyLevel();

        public int ReadTimeoutMillis => _profile?.ReadTimeoutMillis ?? _defaultProfile?.ReadTimeoutMillis ?? _socketOptions.ReadTimeoutMillis;

        public ILoadBalancingPolicy LoadBalancingPolicy => _profile?.LoadBalancingPolicy ?? _defaultProfile?.LoadBalancingPolicy ?? _policies.LoadBalancingPolicy;

        public ISpeculativeExecutionPolicy SpeculativeExecutionPolicy => _profile?.SpeculativeExecutionPolicy ?? _defaultProfile?.SpeculativeExecutionPolicy ?? _policies.SpeculativeExecutionPolicy;

        public IExtendedRetryPolicy RetryPolicy => _profile?.RetryPolicy ?? _defaultProfile?.RetryPolicy ?? _policies.ExtendedRetryPolicy;

        public GraphOptions GraphOptions => _profile?.GraphOptions ?? _defaultProfile?.GraphOptions ?? _graphOptions;

        //// next settings don't exist in execution profiles

        public int PageSize => _queryOptions.GetPageSize();

        public ITimestampGenerator TimestampGenerator => _policies.TimestampGenerator;

        public bool DefaultIdempotence => _queryOptions.GetDefaultIdempotence();

        public int QueryAbortTimeout => _clientOptions.QueryAbortTimeout;

        /// <inheritdoc />
        public ConsistencyLevel GetSerialConsistencyLevelOrDefault(IStatement statement)
        {
            var consistency = SerialConsistencyLevel;
            if (statement.SerialConsistencyLevel != ConsistencyLevel.Any)
            {
                consistency = statement.SerialConsistencyLevel;
            }

            if (!consistency.IsSerialConsistencyLevel())
            {
                throw new ArgumentException("Serial consistency level can only be set to LocalSerial or Serial");
            }

            return consistency;
        }
        
        /// <summary>
        /// Returns the timeout in milliseconds based on the amount of queries.
        /// </summary>
        public int GetQueryAbortTimeout(int amountOfQueries)
        {
            if (amountOfQueries <= 0)
            {
                throw new ArgumentException("The amount of queries must be a positive number");
            }

            if (QueryAbortTimeout == Timeout.Infinite)
            {
                return QueryAbortTimeout;
            }

            return QueryAbortTimeout*amountOfQueries;
        }
    }
}