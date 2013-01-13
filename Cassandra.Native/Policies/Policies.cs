namespace Cassandra
{
    /**
     * Policies configured for a {@link CassandraCluster} instance.
     */
    public class Policies
    {
        /**
         * The default load balancing policy.
         * <p>
         * The default load balancing policy is {@link RoundRobinPolicy}.
         */
        public static readonly LoadBalancingPolicy DefaultLoadBalancingPolicy = new RoundRobinPolicy();

        /**
         * The default reconnection policy.
         * <p>
         * The default reconnetion policy is an {@link ExponentialReconnectionPolicy}
         * where the base delay is 1 second and the max delay is 10 minutes;
         */
        public static readonly ReconnectionPolicy DefaultReconnectionPolicy = new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);

        /**
         * The default retry policy.
         * <p>
         * The default retry policy is {@link DefaultRetryPolicy}.
         */
        public static readonly RetryPolicy DefaultRetryPolicy = Cassandra.DefaultRetryPolicy.Instance;


        public static readonly Policies DefaultPolicies = new Policies(DefaultLoadBalancingPolicy, DefaultReconnectionPolicy, DefaultRetryPolicy);

        private readonly LoadBalancingPolicy _loadBalancingPolicy;
        private readonly ReconnectionPolicy _reconnectionPolicy;
        private readonly RetryPolicy _retryPolicy;

        /**
         * Creates a new {@code Policies} object using the provided policies.
         *
         * @param loadBalancingPolicy the load balancing policy to use.
         * @param reconnectionPolicy the reconnection policy to use.
         * @param retryPolicy the retry policy to use.
         */
        public Policies(LoadBalancingPolicy loadBalancingPolicy,
                        ReconnectionPolicy reconnectionPolicy,
                        RetryPolicy retryPolicy)
        {

            this._loadBalancingPolicy = loadBalancingPolicy;
            this._reconnectionPolicy = reconnectionPolicy;
            this._retryPolicy = retryPolicy;
        }

        /**
         * The load balancing policy in use.
         * <p>
         * The load balancing policy defines how Cassandra hosts are picked for queries.
         *
         * @return the load balancing policy in use.
         */
        public LoadBalancingPolicy LoadBalancingPolicy
        {
            get
            {
                return _loadBalancingPolicy;
            }
        }

        /**
         * The reconnection policy in use.
         * <p>
         * The reconnection policy defines how often the driver tries to reconnect to a dead node.
         *
         * @return the reconnection policy in use.
         */
        public ReconnectionPolicy ReconnectionPolicy
        {
            get
            {
                return _reconnectionPolicy;
            }
        }

        /**
         * The retry policy in use.
         * <p>
         * The retry policy defines in which conditions a query should be
         * automatically retries by the driver.
         *
         * @return the retry policy in use.
         */
        public RetryPolicy RetryPolicy
        {
            get
            {
                return _retryPolicy;
            }
        }
    }
}
