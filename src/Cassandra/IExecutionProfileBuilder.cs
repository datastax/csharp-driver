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

namespace Cassandra
{
    /// <summary>
    /// Builder class that offers a fluent API to build execution profile instances.
    /// The driver provides a default implementation of this interface with the method <see cref="Builder.ExecutionProfileBuilder"/>.
    /// </summary>
    public interface IExecutionProfileBuilder
    {
        /// <summary>
        /// Sets the load balancing policy.
        /// See <see cref="Builder.WithLoadBalancingPolicy"/> and <see cref="ILoadBalancingPolicy"/>
        /// for additional context on this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy);
        
        /// <summary>
        /// Sets the retry policy.
        /// See <see cref="Builder.WithRetryPolicy"/>, <see cref="IRetryPolicy"/> and <see cref="IRetryPolicy"/>
        /// for additional context on this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithRetryPolicy(IExtendedRetryPolicy retryPolicy);
        
        /// <summary>
        /// Sets the speculative execution policy. 
        /// See <see cref="Builder.WithSpeculativeExecutionPolicy"/> and <see cref="ISpeculativeExecutionPolicy"/>
        /// for additional context on this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy speculativeExecutionPolicy);
        
        /// <summary>
        /// Sets the <code>SerialConsistencyLevel</code> setting.
        /// See <see cref="QueryOptions.SetConsistencyLevel"/>, <see cref="QueryOptions.GetConsistencyLevel"/> and
        /// <see cref="ConsistencyLevel"/> for additional context on this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithConsistencyLevel(ConsistencyLevel consistencyLevel);
        
        /// <summary>
        /// Sets the <code>SerialConsistencyLevel</code> setting.
        /// See <see cref="QueryOptions.SetSerialConsistencyLevel"/> and <see cref="QueryOptions.GetSerialConsistencyLevel"/> and
        /// <see cref="ConsistencyLevel"/> for additional context on this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel);
        
        /// <summary>
        /// Sets the <code>ReadTimeoutMillis</code> setting.
        /// See <see cref="SocketOptions.SetReadTimeoutMillis"/> and <see cref="SocketOptions.ReadTimeoutMillis"/>
        /// for a description of this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithReadTimeoutMillis(int readTimeoutMillis);
        
        /// <summary>
        /// Build a new execution profile instance that is configured with the options that were set with this builder.
        /// </summary>
        /// <returns>The new execution profile instance.</returns>
        IExecutionProfile Build();
    }
}