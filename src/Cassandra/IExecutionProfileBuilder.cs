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
    /// <para>
    /// Builder that offers a fluent API to build execution profile instances.
    /// </para>
    /// <para>
    /// If an option is not set, then it will be inherited from the default profile. It is possible to configure
    /// the default profile by providing the "default" name when configuring execution profiles in the Builder.
    /// </para>
    /// <para>
    /// The default profile inherits all settings from the builder level settings.
    /// </para>
    /// <para>
    /// Check the API documentation of each method on this interface
    /// to see what builder setting is used as the default for that particular setting.
    /// </para>
    /// </summary>
    public interface IExecutionProfileBuilder
    {
        /// <summary>
        /// <para>
        /// Sets the load balancing policy.
        /// See <see cref="ILoadBalancingPolicy"/> for additional context on this setting.
        /// </para>
        /// <para>
        /// If no load balancing policy is set through this method, then it will inherit from one of two places:
        /// <list type="bullet">
        /// <item>
        /// <term></term><description>If this is the "default" profile, it will inherit the policy that was specified in <see cref="Builder.WithLoadBalancingPolicy"/>;</description>
        /// </item>
        /// <item>
        /// <term></term><description>If this is not the "default" profile, it will inherit the default profile's policy.</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy);
        
        /// <summary>
        /// <para>
        /// Sets the retry policy.
        /// See <see cref="IRetryPolicy"/> and <see cref="IRetryPolicy"/> for additional context on this setting.
        /// </para>
        /// <para>
        /// If no retry policy is set through this method, then it will inherit from one of two places:
        /// <list type="bullet">
        /// <item>
        /// <term></term><description>If this is the "default" profile, it will inherit the policy that was specified in <see cref="Builder.WithRetryPolicy"/>;</description>
        /// </item>
        /// <item>
        /// <term></term><description>If this is not the "default" profile, it will inherit the default profile's policy.</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithRetryPolicy(IExtendedRetryPolicy retryPolicy);
        
        /// <summary>
        /// <para>
        /// Sets the speculative execution policy. 
        /// See <see cref="ISpeculativeExecutionPolicy"/> for additional context on this setting.
        /// </para>
        /// <para>
        /// If no retry policy is set through this method, then it will inherit from one of two places:
        /// <list type="bullet">
        /// <item>
        /// <term></term><description>If this is the "default" profile, it will inherit the policy that was specified in <see cref="Builder.WithSpeculativeExecutionPolicy"/>;</description>
        /// </item>
        /// <item>
        /// <term></term><description>If this is not the "default" profile, it will inherit the default profile's policy.</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy speculativeExecutionPolicy);
        
        /// <summary>
        /// <para>
        /// Sets the SerialConsistencyLevel setting.
        /// See <see cref="ConsistencyLevel"/> for additional context on this setting.
        /// </para>
        /// <para>
        /// If no value is set through this method, then it will inherit from one of two places:
        /// <list type="bullet">
        /// <item>
        /// <term></term><description>If this is the "default" profile, it will inherit the value that was specified in <see cref="Builder.WithQueryOptions"/>;</description>
        /// </item>
        /// <item>
        /// <term></term><description>If this is not the "default" profile, it will inherit the default profile's value.</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithConsistencyLevel(ConsistencyLevel consistencyLevel);
        
        /// <summary>
        /// <para>
        /// Sets the ConsistencyLevel setting.
        /// See <see cref="ConsistencyLevel"/> for additional context on this setting.
        /// </para>
        /// <para>
        /// If no value is set through this method, then it will inherit from one of two places:
        /// <list type="bullet">
        /// <item>
        /// <term></term><description>If this is the "default" profile, it will inherit the value that was specified in <see cref="Builder.WithQueryOptions"/>;</description>
        /// </item>
        /// <item>
        /// <term></term><description>If this is not the "default" profile, it will inherit the default profile's value.</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel);
        
        /// <summary>
        /// <para>Sets the "ReadTimeoutMillis" setting which is the per-host read timeout in milliseconds.</para>
        /// <para>When setting this value, keep in mind the following:</para>
        /// <para>- the timeout settings used on the Cassandra side (*_request_timeout_in_ms in cassandra.yaml) should be taken into account when picking a value for this read timeout.
        /// In particular, if this read timeout option is lower than Cassandra's timeout, the driver might assume that the host is not responsive and mark it down.</para>
        /// <para>- the read timeout is only approximate and only control the timeout to one Cassandra host, not the full query.</para>
        /// <para>Setting a value of 0 disables client read timeouts.</para>
        /// <para>
        /// If no value is set through this method, then it will inherit from one of two places:
        /// <list type="bullet">
        /// <item>
        /// <term></term><description>If this is the "default" profile, it will inherit the value that was specified in <see cref="Builder.WithSocketOptions"/>;</description>
        /// </item>
        /// <item>
        /// <term></term><description>If this is not the "default" profile, it will inherit the default profile's value.</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <returns>This builder.</returns>
        IExecutionProfileBuilder WithReadTimeoutMillis(int readTimeoutMillis);

        /// <summary>
        /// <para>Sets the DSE Graph options.</para>
        /// <para>See <see cref="GraphOptions"/> for additional information on the settings within the <see cref="GraphOptions"/> class.</para>
        /// <para>
        /// If no options are set through this method, then it will inherit from one of two places:
        /// <list type="bullet">
        /// <item>
        /// <term></term><description>If this is the "default" profile, it will inherit the options that were specified in <see cref="Builder.WithGraphOptions"/>;</description>
        /// </item>
        /// <item>
        /// <term></term><description>If this is not the "default" profile, it will inherit the default profile's options.</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        IExecutionProfileBuilder WithGraphOptions(GraphOptions graphOptions);
    }
}