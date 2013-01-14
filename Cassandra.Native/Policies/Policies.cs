namespace Cassandra
{
    /// <summary>
    ///  Policies configured for a <link>Cluster</link>
    ///  instance.
    /// </summary>
    public class Policies
    {
        /// <summary>
        ///  The default load balancing policy. <p> The default load balancing policy is
        ///  <link>RoundRobinPolicy</link>.</p>
        /// </summary>
        public static readonly ILoadBalancingPolicy DefaultLoadBalancingPolicy = new RoundRobinPolicy();

        /// <summary>
        ///  The default reconnection policy. <p> The default reconnetion policy is an
        ///  <link>ExponentialReconnectionPolicy</link> where the base delay is 1 second
        ///  and the max delay is 10 minutes;</p>
        /// </summary>
        public static readonly IReconnectionPolicy DefaultReconnectionPolicy = new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);

        /// <summary>
        ///  The default retry policy. <p> The default retry policy is
        ///  <link>DefaultRetryPolicy</link>.</p>
        /// </summary>
        public static readonly IRetryPolicy DefaultRetryPolicy = Cassandra.DefaultRetryPolicy.Instance;


        public static readonly Policies DefaultPolicies = new Policies(DefaultLoadBalancingPolicy, DefaultReconnectionPolicy, DefaultRetryPolicy);

        private readonly ILoadBalancingPolicy _loadBalancingPolicy;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private readonly IRetryPolicy _retryPolicy;

        /// <summary>
        ///  Creates a new <code>Policies</code> object using the provided policies.
        /// </summary>
        /// <param name="loadBalancingPolicy"> the load balancing policy to use. </param>
        /// <param name="reconnectionPolicy"> the reconnection policy to use. </param>
        /// <param name="retryPolicy"> the retry policy to use.</param>
        public Policies(ILoadBalancingPolicy loadBalancingPolicy,
                        IReconnectionPolicy reconnectionPolicy,
                        IRetryPolicy retryPolicy)
        {

            this._loadBalancingPolicy = loadBalancingPolicy;
            this._reconnectionPolicy = reconnectionPolicy;
            this._retryPolicy = retryPolicy;
        }

        /// <summary>
        ///  Gets the load balancing policy in use. <p> The load balancing policy defines how
        ///  Cassandra hosts are picked for queries.</p>
        /// </summary>
        public ILoadBalancingPolicy LoadBalancingPolicy
        {
            get
            {
                return _loadBalancingPolicy;
            }
        }

        /// <summary>
        ///  Gets the reconnection policy in use. <p> The reconnection policy defines how often
        ///  the driver tries to reconnect to a dead node.</p>
        /// </summary>
        public IReconnectionPolicy ReconnectionPolicy
        {
            get
            {
                return _reconnectionPolicy;
            }
        }

        /// <summary>
        ///  Gets the retry policy in use. <p> The retry policy defines in which conditions a
        ///  query should be automatically retries by the driver.</p>
        /// </summary>
        public IRetryPolicy RetryPolicy
        {
            get
            {
                return _retryPolicy;
            }
        }
    }
}
