//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    /// <summary>
    ///  Policies configured for a <link>Cluster</link>
    ///  instance.
    /// </summary>
    public class Policies
    {
        /// <summary>
        ///  The default load balancing policy. 
        /// <para> 
        /// The default load balancing policy is <see cref="TokenAwarePolicy"/> with <see cref="DCAwareRoundRobinPolicy"/> as child policy.
        /// </para>
        /// </summary>
        public static ILoadBalancingPolicy DefaultLoadBalancingPolicy
        {
            get
            {
                return new TokenAwarePolicy(new DCAwareRoundRobinPolicy());
            }
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
        ///  The default retry policy. <p> The default retry policy is
        ///  <link>DefaultRetryPolicy</link>.</p>
        /// </summary>
        public static IRetryPolicy DefaultRetryPolicy
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

        public static Policies DefaultPolicies
        {
            get
            {
                return new Policies(DefaultLoadBalancingPolicy, DefaultReconnectionPolicy, DefaultRetryPolicy, DefaultSpeculativeExecutionPolicy);
            }
        }

        private readonly ILoadBalancingPolicy _loadBalancingPolicy;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private readonly IRetryPolicy _retryPolicy;
        private readonly ISpeculativeExecutionPolicy _speculativeExecutionPolicy;

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

        public Policies()
        {
        }

        /// <summary>
        ///  Creates a new <c>Policies</c> object using the provided policies.
        /// </summary>
        /// <param name="loadBalancingPolicy"> the load balancing policy to use. </param>
        /// <param name="reconnectionPolicy"> the reconnection policy to use. </param>
        /// <param name="retryPolicy"> the retry policy to use.</param>
        public Policies(ILoadBalancingPolicy loadBalancingPolicy,
                        IReconnectionPolicy reconnectionPolicy,
                        IRetryPolicy retryPolicy)
            : this(loadBalancingPolicy, reconnectionPolicy, retryPolicy, NoSpeculativeExecutionPolicy.Instance)
        {
            //Part of the public API can not be removed
        }

        internal Policies(ILoadBalancingPolicy loadBalancingPolicy,
            IReconnectionPolicy reconnectionPolicy,
            IRetryPolicy retryPolicy,
            ISpeculativeExecutionPolicy speculativeExecutionPolicy)
        {
            _loadBalancingPolicy = loadBalancingPolicy;
            _reconnectionPolicy = reconnectionPolicy;
            _retryPolicy = retryPolicy;
            _speculativeExecutionPolicy = speculativeExecutionPolicy;
        }
    }
}
