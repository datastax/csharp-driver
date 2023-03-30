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

namespace Cassandra
{
    /// <summary>
    /// Represents the policies configured for a <see cref="ICluster"/> instance.
    /// </summary>
    public class Policies
    {
        /// <summary>
        /// <para>
        /// DEPRECATED: Use <see cref="NewDefaultLoadBalancingPolicy"/> instead. Providing the local datacenter will be mandatory
        /// in the next major version of the driver.
        /// </para>
        /// <para>
        /// The default load balancing policy.
        /// </para>  
        /// <para> 
        /// The default load balancing policy is <see cref="DefaultLoadBalancingPolicy"/> as a wrapper around
        /// <see cref="TokenAwarePolicy"/> with <see cref="DCAwareRoundRobinPolicy"/> as child policy.
        /// </para>
        /// </summary>
        public static ILoadBalancingPolicy DefaultLoadBalancingPolicy => 
            new DefaultLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()));

        /// <summary>
        /// Creates a new instance of the default load balancing policy with the provided local datacenter.
        /// This is equivalent to:
        /// <code>
        /// new DefaultLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc)))
        /// </code>
        /// </summary>
        public static ILoadBalancingPolicy NewDefaultLoadBalancingPolicy(string localDc)
        {
#pragma warning disable 618
            return new DefaultLoadBalancingPolicy(localDc);
#pragma warning restore 618
        }

        /// <summary>
        ///  The default reconnection policy. <p> The default reconnection policy is an
        ///  <link>ExponentialReconnectionPolicy</link> where the base delay is 1 second
        ///  and the max delay is 10 minutes;</p>
        /// </summary>
        public static IReconnectionPolicy DefaultReconnectionPolicy
        {
            get
            {
                return new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);
            }
        }
        
        /// <summary>
        ///  The default retry policy.The default retry policy is <see cref="Cassandra.DefaultRetryPolicy"/>
        /// </summary>
        public static IRetryPolicy DefaultRetryPolicy
        {
            get
            {
                return new DefaultRetryPolicy();
            }
        }
        
        /// <summary>
        ///  The default extended retry policy.The default extended retry policy is <see cref="Cassandra.DefaultRetryPolicy"/>
        /// </summary>
        internal static IExtendedRetryPolicy DefaultExtendedRetryPolicy
        {
            get
            {
                return new DefaultRetryPolicy();
            }
        }

        /// <summary>
        /// The <see cref="ISpeculativeExecutionPolicy"/> to be used by default.
        /// <para> 
        /// The default is <see cref="NoSpeculativeExecutionPolicy"/>.
        /// </para>
        /// </summary>
        public static ISpeculativeExecutionPolicy DefaultSpeculativeExecutionPolicy
        {
            get
            {
                return NoSpeculativeExecutionPolicy.Instance;
            }
        }

        /// <summary>
        /// Gets a new instance of the default <see cref="ITimestampGenerator"/> policy.
        /// <para>
        /// The default <see cref="ITimestampGenerator"/> is <see cref="AtomicMonotonicTimestampGenerator"/>
        /// </para>
        /// </summary>
        public static ITimestampGenerator DefaultTimestampGenerator
        {
            get { return new AtomicMonotonicTimestampGenerator(); }
        }

        /// <summary>
        /// Gets a new instance <see cref="Policies"/> containing default policies of the driver.
        /// </summary>
        public static Policies DefaultPolicies
        {
            get
            {
                return new Policies();
            }
        }

        private readonly ILoadBalancingPolicy _loadBalancingPolicy;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private readonly IRetryPolicy _retryPolicy;
        private readonly ISpeculativeExecutionPolicy _speculativeExecutionPolicy;
        private IExtendedRetryPolicy _extendedRetryPolicy;
        private readonly ITimestampGenerator _timestampGenerator;

        /// <summary>
        ///  Gets the load balancing policy in use. <p> The load balancing policy defines how
        ///  Cassandra hosts are picked for queries.</p>
        /// </summary>
        public ILoadBalancingPolicy LoadBalancingPolicy
        {
            get { return _loadBalancingPolicy; }
        }

        /// <summary>
        ///  Gets the reconnection policy in use. <p> The reconnection policy defines how often
        ///  the driver tries to reconnect to a dead node.</p>
        /// </summary>
        public IReconnectionPolicy ReconnectionPolicy
        {
            get { return _reconnectionPolicy; }
        }

        /// <summary>
        ///  Gets the retry policy in use. <p> The retry policy defines in which conditions a
        ///  query should be automatically retries by the driver.</p>
        /// </summary>
        public IRetryPolicy RetryPolicy
        {
            get { return _retryPolicy; }
        }

        /// <summary>
        /// Gets the <see cref="SpeculativeExecutionPolicy"/> in use.
        /// </summary>
        public ISpeculativeExecutionPolicy SpeculativeExecutionPolicy
        {
            get { return _speculativeExecutionPolicy; }
        }

        /// <summary>
        /// Gets the extended retry policy that contains the default behavior to handle request errors.
        /// The returned value is either the same instance as <see cref="RetryPolicy"/> or the default
        /// retry policy. It can not be null.
        /// </summary>
        internal IExtendedRetryPolicy ExtendedRetryPolicy
        {
            get { return _extendedRetryPolicy; }
        }

        /// <summary>
        /// Gets the <see cref="ITimestampGenerator"/> instance in use.
        /// </summary>
        public ITimestampGenerator TimestampGenerator
        {
            get { return _timestampGenerator; }
        }

        public Policies() : this(null, null, null, null, null)
        {
            //Part of the public API can not be removed
        }

        /// <summary>
        /// Creates a new <c>Policies</c> object using the provided policies.
        /// </summary>
        /// <param name="loadBalancingPolicy"> the load balancing policy to use. </param>
        /// <param name="reconnectionPolicy"> the reconnection policy to use. </param>
        /// <param name="retryPolicy"> the retry policy to use.</param>
        public Policies(ILoadBalancingPolicy loadBalancingPolicy, IReconnectionPolicy reconnectionPolicy, IRetryPolicy retryPolicy)
            : this(loadBalancingPolicy, reconnectionPolicy, retryPolicy, DefaultSpeculativeExecutionPolicy, DefaultTimestampGenerator)
        {
            //Part of the public API can not be removed
        }

        internal Policies(ILoadBalancingPolicy loadBalancingPolicy,
            IReconnectionPolicy reconnectionPolicy,
            IRetryPolicy retryPolicy,
            ISpeculativeExecutionPolicy speculativeExecutionPolicy,
            ITimestampGenerator timestampGenerator)
        {
            _loadBalancingPolicy = loadBalancingPolicy ?? DefaultLoadBalancingPolicy;
            _reconnectionPolicy = reconnectionPolicy ?? DefaultReconnectionPolicy;
            _retryPolicy = retryPolicy ?? DefaultRetryPolicy;
            _speculativeExecutionPolicy = speculativeExecutionPolicy ?? DefaultSpeculativeExecutionPolicy;
            _timestampGenerator = timestampGenerator ?? DefaultTimestampGenerator;
            _extendedRetryPolicy = _retryPolicy.Wrap(DefaultExtendedRetryPolicy);
        }

        private Policies(
            ILoadBalancingPolicy loadBalancingPolicy,
            IReconnectionPolicy reconnectionPolicy,
            IRetryPolicy retryPolicy,
            ISpeculativeExecutionPolicy speculativeExecutionPolicy,
            ITimestampGenerator timestampGenerator,
            IExtendedRetryPolicy extendedRetryPolicy)
        {
            _loadBalancingPolicy = loadBalancingPolicy ?? throw new ArgumentNullException(nameof(loadBalancingPolicy));
            _reconnectionPolicy = reconnectionPolicy ?? throw new ArgumentNullException(nameof(reconnectionPolicy));
            _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
            _speculativeExecutionPolicy = speculativeExecutionPolicy ?? throw new ArgumentNullException(nameof(speculativeExecutionPolicy));
            _timestampGenerator = timestampGenerator ?? throw new ArgumentNullException(nameof(timestampGenerator));
            _extendedRetryPolicy = extendedRetryPolicy ?? throw new ArgumentNullException(nameof(extendedRetryPolicy));
        }

        internal Policies CloneWithExecutionProfilePolicies(IExecutionProfile defaultProfile)
        {
            return new Policies(
                defaultProfile.LoadBalancingPolicy ?? _loadBalancingPolicy,
                _reconnectionPolicy,
                _retryPolicy,
                defaultProfile.SpeculativeExecutionPolicy ?? _speculativeExecutionPolicy,
                _timestampGenerator,
                _extendedRetryPolicy);
        }
    }
}
