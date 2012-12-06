using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native.Policies
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
        public static readonly LoadBalancingPolicy DEFAULT_LOAD_BALANCING_POLICY = new RoundRobinPolicy();

        /**
         * The default reconnection policy.
         * <p>
         * The default reconnetion policy is an {@link ExponentialReconnectionPolicy}
         * where the base delay is 1 second and the max delay is 10 minutes;
         */
        public static readonly ReconnectionPolicy DEFAULT_RECONNECTION_POLICY = new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);

        /**
         * The default retry policy.
         * <p>
         * The default retry policy is {@link DefaultRetryPolicy}.
         */
        public static readonly RetryPolicy DEFAULT_RETRY_POLICY = DefaultRetryPolicy.INSTANCE;


        public static readonly Policies DEFAULT_POLICIES = new Policies(DEFAULT_LOAD_BALANCING_POLICY, DEFAULT_RECONNECTION_POLICY, DEFAULT_RETRY_POLICY);

        private readonly LoadBalancingPolicy loadBalancingPolicy;
        private readonly ReconnectionPolicy reconnectionPolicy;
        private readonly RetryPolicy retryPolicy;

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

            this.loadBalancingPolicy = loadBalancingPolicy;
            this.reconnectionPolicy = reconnectionPolicy;
            this.retryPolicy = retryPolicy;
        }

        /**
         * The load balancing policy in use.
         * <p>
         * The load balancing policy defines how Cassandra hosts are picked for queries.
         *
         * @return the load balancing policy in use.
         */
        public LoadBalancingPolicy getLoadBalancingPolicy()
        {
            return loadBalancingPolicy;
        }

        /**
         * The reconnection policy in use.
         * <p>
         * The reconnection policy defines how often the driver tries to reconnect to a dead node.
         *
         * @return the reconnection policy in use.
         */
        public ReconnectionPolicy getReconnectionPolicy()
        {
            return reconnectionPolicy;
        }

        /**
         * The retry policy in use.
         * <p>
         * The retry policy defines in which conditions a query should be
         * automatically retries by the driver.
         *
         * @return the retry policy in use.
         */
        public RetryPolicy getRetryPolicy()
        {
            return retryPolicy;
        }
    }
}
