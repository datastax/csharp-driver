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
using System.Threading;

namespace Cassandra.ExecutionProfiles
{
    /// <summary>
    /// This class differs from <see cref="ExecutionProfile"/> in the sense that there are no nullable properties and it contains
    /// some additional properties that do not exist in execution profiles but are set per request (and usually settable in Statements).
    /// Every property is guaranteed to have a value, either from a user provided profile or from the cluster config.
    /// </summary>
    internal class RequestOptions : IRequestOptions
    {
        private readonly ExecutionProfile _profile;
        private readonly Policies _policies;
        private readonly SocketOptions _socketOptions;
        private readonly QueryOptions _queryOptions;
        private readonly ClientOptions _clientOptions;

        /// <summary>
        /// Builds a request options object without any null settings.
        /// </summary>
        /// <param name="profile">Can be null.</param>
        /// <param name="policies">Must not be null and the inner policy settings must not be null either.</param>
        /// <param name="socketOptions">Must not be null.</param>
        /// <param name="queryOptions">Must not be null.</param>
        /// <param name="clientOptions">Must not be null.</param>
        public RequestOptions(ExecutionProfile profile, Policies policies, SocketOptions socketOptions, QueryOptions queryOptions, ClientOptions clientOptions)
        {
            _profile = profile;
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

        public ConsistencyLevel ConsistencyLevel => _profile?.ConsistencyLevel ?? _queryOptions.GetConsistencyLevel();

        public ConsistencyLevel SerialConsistencyLevel => _profile?.SerialConsistencyLevel ?? _queryOptions.GetSerialConsistencyLevel();

        public int ReadTimeoutMillis => _profile?.ReadTimeoutMillis ?? _socketOptions.ReadTimeoutMillis;

        public ILoadBalancingPolicy LoadBalancingPolicy => _profile?.LoadBalancingPolicy ?? _policies.LoadBalancingPolicy;

        public ISpeculativeExecutionPolicy SpeculativeExecutionPolicy => _profile?.SpeculativeExecutionPolicy ?? _policies.SpeculativeExecutionPolicy;

        public IExtendedRetryPolicy RetryPolicy => _profile?.RetryPolicy ?? _policies.ExtendedRetryPolicy;

        //// next settings don't exist in execution profiles

        public int PageSize => _queryOptions.GetPageSize();

        public ITimestampGenerator TimestampGenerator => _policies.TimestampGenerator;

        public bool DefaultIdempotence => _queryOptions.GetDefaultIdempotence();

        public int QueryAbortTimeout => _clientOptions.QueryAbortTimeout;

        public bool PrepareOnAllHosts => _queryOptions.IsPrepareOnAllHosts();

        public bool ReprepareOnUp => _queryOptions.IsReprepareOnUp();

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