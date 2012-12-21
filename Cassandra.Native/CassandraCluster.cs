using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using Cassandra;
using Cassandra.Native;

namespace Cassandra
{
    /**
     * Informations and known state of a Cassandra cluster.
     * <p>
     * This is the main entry point of the driver. A simple example of access to a
     * Cassandra cluster would be:
     * <pre>
     *   Cluster cluster = Cluster.CassandraClusterBuilder().addContactPoint("192.168.0.1").build();
     *   Session session = cluster.connect("db1");
     *
     *   for (Row row : session.execute("SELECT * FROM table1"))
     *       // do something ...
     * </pre>
     * <p>
     * A cluster object maintains a permanent connection to one of the cluster node
     * that it uses solely to maintain informations on the state and current
     * topology of the cluster. Using the connection, the driver will discover all
     * the nodes composing the cluster as well as new nodes joining the cluster.
     */
    public class Cluster
    {
        public const int DEFAULT_PORT = 9042;

        IEnumerable<IPAddress> contactPoints;
        int port;
        Policies policies;
        AuthInfoProvider credentialsDelegate = null;
        bool noBufferingIfPossible;

        PoolingOptions poolingOptions = new PoolingOptions();
        public PoolingOptions PoolingOptions { get { return poolingOptions; } }

        CompressionType compression = CompressionType.NoCompression;
        public CompressionType Compression { get { return compression; } }

        private Cluster(IEnumerable<IPAddress> contactPoints, int port, Policies policies, AuthInfoProvider credentialsDelegate = null, bool noBufferingIfPossible = false, CompressionType compression = CompressionType.NoCompression)
        {
            this.contactPoints = contactPoints;
            this.port = port;
            this.policies = policies;
            this.credentialsDelegate = credentialsDelegate;
            this.noBufferingIfPossible = noBufferingIfPossible;
            this.compression = compression;
        }

        /**
         * Build a new cluster based on the provided initializer.
         * <p>
         * Note that for building a cluster programmatically, Cluster.CassandraClusterBuilder
         * provides a slightly less verbose shortcut with {@link CassandraClusterBuilder#build}.
         * <p>
         * Also note that that all the contact points provided by {@code
         * initializer} must share the same port.
         *
         * @param initializer the Cluster.Initializer to use
         * @return the newly created Cluster instance
         *
         * @throws NoHostAvailableException if no host amongst the contact points
         * can be reached.
         * @throws IllegalArgumentException if the list of contact points provided
         * by {@code initiazer} is empty or if not all those contact points have the same port.
         * @throws AuthenticationException if while contacting the initial
         * contact points an authencation error occurs.
         */
        public static Cluster BuildFrom(Initializer initializer)
        {
            IEnumerable<IPAddress> contactPoints = initializer.ContactPoints;
            //if (contactPoints.)
            //    throw new IllegalArgumentException("Cannot build a cluster without contact points");

            return new Cluster(contactPoints, initializer.Port, initializer.Policies, initializer.AuthInfoProvider, initializer.UseNoBufferingIfPossible);
        }

        /**
 * Creates a new {@link Cluster.CassandraClusterBuilder} instance.
 * <p>
 * This is a shortcut for {@code new Cluster.CassandraClusterBuilder()}.
 *
 * @return the new cluster CassandraClusterBuilder.
 */
        public static ClusterBuilder Builder
        {
            get
            {
                return new ClusterBuilder();
            }
        }

        /**
         * Creates a new session on this cluster.
         *
         * @return a new session on this cluster sets to no keyspace.
         */
        public Session Connect()
        {
            return Connect("");
        }

        /**
         * Creates a new session on this cluster and sets a keyspace to use.
         *
         * @param keyspace The name of the keyspace to use for the created
         * {@code Session}.
         * @return a new session on this cluster sets to keyspace
         * {@code keyspaceName}.
         *
         * @throws NoHostAvailableException if no host can be contacted to set the
         * {@code keyspace}.
         */
        public Session Connect(string keyspace)
        {
            return new Session(
                clusterEndpoints: contactPoints,
                port: port,
                keyspace: keyspace,
                credentialsDelegate: credentialsDelegate,
                policies: policies,
                poolingOptions: poolingOptions,
                noBufferingIfPossible: noBufferingIfPossible,
                compression: compression
                );
        }
    }

    /**
     * Initializer for {@link Cluster} instances.
     */
    public interface Initializer
    {

        /**
         * Returns the initial Cassandra hosts to connect to.
         *
         * @return the initial Cassandra contact points. See {@link CassandraClusterBuilder#addContactPoint}
         * for more details on contact points.
         */
        IEnumerable<IPAddress> ContactPoints { get; }

        /**
         * The port to use to connect to Cassandra hosts.
         * <p>
         * This port will be used to connect to all of the Cassandra cluster
         * hosts, not only the contact points. This means that all Cassandra
         * host must be configured to listen on the same port.
         *
         * @return the port to use to connect to Cassandra hosts.
         */
        int Port { get; }

        /**
         * Returns the policies to use for this cluster.
         *
         * @return the policies to use for this cluster.
         */
        Policies Policies { get; }

        /**
         * The authentication provider to use to connect to the Cassandra cluster.
         *
         * @return the authentication provider to use. Use
         * AuthInfoProvider.NONE if authentication is not to be used.
         */
        AuthInfoProvider AuthInfoProvider { get; }

        bool UseNoBufferingIfPossible { get; }
    }

    /**
     * Helper class to build {@link Cluster} instances.
     */
    public class ClusterBuilder : Initializer
    {

        private readonly List<IPAddress> addresses = new List<IPAddress>();
        private int port = Cluster.DEFAULT_PORT;
        private AuthInfoProvider authProvider = null;
        private CompressionType compression = CompressionType.NoCompression;

        private LoadBalancingPolicy loadBalancingPolicy;
        private ReconnectionPolicy reconnectionPolicy;
        private RetryPolicy retryPolicy;
        private bool noBufferingIfPossible = false;

        public IEnumerable<IPAddress> ContactPoints
        {
            get
            {
                return addresses;
            }
        }

        public ClusterBuilder WithConnectionString(string connectionString)
        {
            ConnectionStringBuilder cnb = new ConnectionStringBuilder(connectionString);

            foreach (var addr in cnb.ContactPoints)
                AddContactPoints(addr);
            WithPort(cnb.Port);
            return this;
        }
        
        /**
         * The port to use to connect to the Cassandra host.
         *
         * If not set through this method, the default port (9042) will be used
         * instead.
         *
         * @param port the port to set.
         * @return this CassandraClusterBuilder
         */
        public ClusterBuilder WithPort(int port)
        {
            this.port = port;
            return this;
        }


        /**
         * Sets the compression to use for the transport.
         *
         * @param compression the compression to set
         * @return this Builder
         *
         * @see ProtocolOptions.Compression
         */
        public ClusterBuilder withCompression(CompressionType compression)
        {
            this.compression = compression;
            return this;
        }

        public ClusterBuilder OmmitBufferingIfPossible()
        {
            this.noBufferingIfPossible = true;
            return this;
        }

        /**
         * The port to use to connect to Cassandra hosts.
         *
         * @return the port to use to connect to Cassandra hosts.
         */
        public int Port
        {
            get
            {
                return port;
            }
        }

        /**
         * Adds a contact point.
         *
         * Contact points are addresses of Cassandra nodes that the driver uses
         * to discover the cluster topology. Only one contact point is required
         * (the driver will retrieve the address of the other nodes
         * automatically), but it is usually a good idea to provide more than
         * one contact point, as if that unique contact point is not available,
         * the driver won't be able to initialize itself correctly.
         *
         * @param address the address of the node to connect to
         * @return this CassandraClusterBuilder
         *
         * @throws IllegalArgumentException if no IP address for {@code address}
         * could be found
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         */
        public ClusterBuilder AddContactPoint(string address)
        {
            this.addresses.Add(IPAddress.Parse(address));
            return this;
        }

        /**
         * Add contact points.
         *
         * See {@link CassandraClusterBuilder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point
         * @return this CassandraClusterBuilder
         *
         * @throws IllegalArgumentException if no IP address for at least one
         * of {@code addresses} could be found
         * @throws SecurityException if a security manager is present and
         * permission to resolve the host name is denied.
         *
         * @see CassandraClusterBuilder#addContactPoint
         */
        public ClusterBuilder AddContactPoints(params string[] addresses)
        {
            foreach (string address in addresses)
                AddContactPoint(address);
            return this;
        }

        /**
         * Add contact points.
         *
         * See {@link CassandraClusterBuilder#addContactPoint} for more details on contact
         * points.
         *
         * @param addresses addresses of the nodes to add as contact point
         * @return this CassandraClusterBuilder
         *
         * @see CassandraClusterBuilder#addContactPoint
         */
        public ClusterBuilder AddContactPoints(params IPAddress[] addresses)
        {
            foreach (IPAddress address in addresses)
                this.addresses.Add(address);
            return this;
        }

        /**
         * Configure the load balancing policy to use for the new cluster.
         * <p>
         * If no load balancing policy is set through this method,
         * {@link Policies#DEFAULT_LOAD_BALANCING_POLICY} will be used instead.
         *
         * @param policy the load balancing policy to use
         * @return this CassandraClusterBuilder
         */
        public ClusterBuilder WithLoadBalancingPolicy(LoadBalancingPolicy policy)
        {
            this.loadBalancingPolicy = policy;
            return this;
        }

        /**
         * Configure the reconnection policy to use for the new cluster.
         * <p>
         * If no reconnection policy is set through this method,
         * {@link Policies#DEFAULT_RECONNECTION_POLICY} will be used instead.
         *
         * @param policy the reconnection policy to use
         * @return this CassandraClusterBuilder
         */
        public ClusterBuilder WithReconnectionPolicy(ReconnectionPolicy policy)
        {
            this.reconnectionPolicy = policy;
            return this;
        }

        /**
         * Configure the retry policy to use for the new cluster.
         * <p>
         * If no retry policy is set through this method,
         * {@link Policies#DEFAULT_RETRY_POLICY} will be used instead.
         *
         * @param policy the retry policy to use
         * @return this CassandraClusterBuilder
         */
        public ClusterBuilder WithRetryPolicy(RetryPolicy policy)
        {
            this.retryPolicy = policy;
            return this;
        }

        /**
         * Returns the policies to use for this cluster.
         * <p>
         * The policies used are the one set by the {@code with*} methods of
         * this CassandraClusterBuilder, or the default ones defined in {@link Policies} for
         * the policies that hasn't been explicitely set.
         *
         * @return the policies to use for this cluster.
         */
        public Policies Policies
        {
            get
            {
                return new Policies(
                    loadBalancingPolicy == null ? Cassandra.Policies.DEFAULT_LOAD_BALANCING_POLICY : loadBalancingPolicy,
                    reconnectionPolicy == null ? Cassandra.Policies.DEFAULT_RECONNECTION_POLICY : reconnectionPolicy,
                    retryPolicy == null ? Cassandra.Policies.DEFAULT_RETRY_POLICY : retryPolicy
                );
            }
        }

        /**
         * Use the provided {@code AuthInfoProvider} to connect to Cassandra hosts.
         * <p>
         * This is optional if the Cassandra cluster has been configured to not
         * require authentication (the default).
         *
         * @param authInfoProvider the authentication info provider to use
         * @return this CassandraClusterBuilder
         */
        public ClusterBuilder WithAuthInfoProvider(AuthInfoProvider authInfoProvider)
        {
            this.authProvider = authInfoProvider;
            return this;
        }

        /**
         * The authentication provider to use to connect to the Cassandra cluster.
         *
         * @return the authentication provider set through {@link #withAuthInfoProvider}
         * or AuthInfoProvider.NONE if nothing was set.
         */
        public AuthInfoProvider AuthInfoProvider
        {
            get
            {
                return this.authProvider;
            }
        }

        public bool UseNoBufferingIfPossible
        {
            get
            {
                return noBufferingIfPossible;
            }
        }

        /**
         * Build the cluster with the configured set of initial contact points
         * and policies.
         *
         * This is a shorthand for {@code Cluster.buildFrom(this)}.
         *
         * @return the newly build Cluster instance.
         *
         * @throws NoHostAvailableException if none of the contact points
         * provided can be reached.
         * @throws AuthenticationException if while contacting the initial
         * contact points an authencation error occurs.
         */
        public Cluster Build()
        {
            return Cluster.BuildFrom(this);
        }


    }
}
