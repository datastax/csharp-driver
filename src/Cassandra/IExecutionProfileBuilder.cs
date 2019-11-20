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

using Cassandra.DataStax.Graph;

namespace Cassandra
{
    /// <summary>
    /// Builder that offers a fluent API to build execution profile instances.
    /// </summary>
    public interface IExecutionProfileBuilder
    {
        /// <summary>
        /// Sets the load balancing policy.
        /// See <see cref="ILoadBalancingPolicy"/> for additional context on this setting.
        /// If no load balancing policy is set through this method, <see cref="Policies.DefaultLoadBalancingPolicy"/> will be used instead.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy);
        
        /// <summary>
        /// Sets the retry policy.
        /// See <see cref="IRetryPolicy"/> and <see cref="IRetryPolicy"/> for additional context on this setting.
        /// When the retry policy is not set with this method, the <see cref="Policies.DefaultRetryPolicy" />
        /// will be used instead.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithRetryPolicy(IExtendedRetryPolicy retryPolicy);
        
        /// <summary>
        /// Sets the speculative execution policy. 
        /// See <see cref="ISpeculativeExecutionPolicy"/> for additional context on this setting.
        /// If no speculative execution policy is set through this method, <see cref="Policies.DefaultSpeculativeExecutionPolicy"/> will be used instead.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy speculativeExecutionPolicy);
        
        /// <summary>
        /// Sets the <code>SerialConsistencyLevel</code> setting.
        /// See <see cref="ConsistencyLevel"/> for additional context on this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithConsistencyLevel(ConsistencyLevel consistencyLevel);
        
        /// <summary>
        /// Sets the <code>SerialConsistencyLevel</code> setting.
        /// See <see cref="ConsistencyLevel"/> for additional context on this setting.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel);
        
        /// <summary>
        /// <para>Sets the <code>ReadTimeoutMillis</code> setting which is the per-host read timeout in milliseconds.</para>
        /// <para>When setting this value, keep in mind the following:</para>
        /// <para>- the timeout settings used on the Cassandra side (*_request_timeout_in_ms in cassandra.yaml) should be taken into account when picking a value for this read timeout.
        /// In particular, if this read timeout option is lower than Cassandra's timeout, the driver might assume that the host is not responsive and mark it down.</para>
        /// <para>- the read timeout is only approximate and only control the timeout to one Cassandra host, not the full query.</para>
        /// Setting a value of 0 disables client read timeouts.
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithReadTimeoutMillis(int readTimeoutMillis);

        /// <summary>
        /// <para>Sets the DSE Graph options.</para>
        /// <para>See <see cref="GraphOptions"/> for additional information on the settings within the <see cref="GraphOptions"/> class.</para>
        /// </summary>
        IExecutionProfileBuilder WithGraphOptions(GraphOptions graphOptions);
    }
}