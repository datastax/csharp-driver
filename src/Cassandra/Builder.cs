using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  Helper class to build <link>Cluster</link> instances.
    /// </summary>
    public class Builder : IInitializer
    {
        private readonly List<IPAddress> _addresses = new List<IPAddress>();
        private PoolingOptions _poolingOptions = new PoolingOptions();
        private SocketOptions _socketOptions = new SocketOptions();
        private IAuthInfoProvider _authInfoProvider;
        private IAuthProvider _authProvider = NoneAuthProvider.Instance;
        private CompressionType _compression = CompressionType.NoCompression;
        private string _defaultKeyspace;

        private ILoadBalancingPolicy _loadBalancingPolicy;
        private int _port = ProtocolOptions.DefaultPort;
        private int _queryAbortTimeout = Timeout.Infinite;
        private QueryOptions _queryOptions = new QueryOptions();
        private IReconnectionPolicy _reconnectionPolicy;
        private IRetryPolicy _retryPolicy;
        private SSLOptions _sslOptions;
        private bool _withoutRowSetBuffering;

        public int Port
        {
            get { return _port; }
        }

        /// <summary>
        ///  The pooling options used by this builder.
        /// </summary>
        /// 
        /// <returns>the pooling options that will be used by this builder. You can use
        ///  the returned object to define the initial pooling options for the built
        ///  cluster.</returns>
        public PoolingOptions PoolingOptions
        {
            get { return _poolingOptions; }
        }

        /// <summary>
        ///  The socket options used by this builder.
        /// </summary>
        /// 
        /// <returns>the socket options that will be used by this builder. You can use
        ///  the returned object to define the initial socket options for the built
        ///  cluster.</returns>
        public SocketOptions SocketOptions
        {
            get { return _socketOptions; }
        }

        public ICollection<IPAddress> ContactPoints
        {
            get { return _addresses; }
        }

        /// <summary>
        ///  The configuration that will be used for the new cluster. <p> You <b>should
        ///  not</b> modify this object directly as change made to the returned object may
        ///  not be used by the cluster build. Instead, you should use the other methods
        ///  of this <c>Builder</c></p>.
        /// </summary>
        /// 
        /// <returns>the configuration to use for the new cluster.</returns>
        public Configuration GetConfiguration()
        {
            var policies = new Policies(
                _loadBalancingPolicy ?? Policies.DefaultLoadBalancingPolicy,
                _reconnectionPolicy ?? Policies.DefaultReconnectionPolicy,
                _retryPolicy ?? Policies.DefaultRetryPolicy
                );

            return new Configuration(policies,
                                     new ProtocolOptions(_port, _sslOptions).SetCompression(_compression),
                                     _poolingOptions,
                                     _socketOptions,
                                     new ClientOptions(_withoutRowSetBuffering, _queryAbortTimeout, _defaultKeyspace),
                                     _authProvider,
                                     _authInfoProvider,
                                     _queryOptions
                );
        }

        /// <summary>
        ///  The port to use to connect to the Cassandra host. If not set through this
        ///  method, the default port (9042) will be used instead.
        /// </summary>
        /// <param name="port"> the port to set. </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithPort(int port)
        {
            _port = port;
            return this;
        }


        /// <summary>
        /// Sets the QueryOptions to use for the newly created Cluster.
        /// 
        /// If no query options are set through this method, default query
        /// options will be used.
        /// </summary>
        /// <param name="options">the QueryOptions to use.</param>
        /// <returns>this Builder.</returns>
        public Builder WithQueryOptions(QueryOptions options)
        {
            _queryOptions = options;
            return this;
        }

        /// <summary>
        ///  Sets the compression to use for the transport.
        /// </summary>
        /// <param name="compression"> the compression to set </param>
        /// 
        /// <returns>this Builder <see>ProtocolOptions.Compression</see></returns>
        public Builder WithCompression(CompressionType compression)
        {
            _compression = compression;
            return this;
        }

        /// <summary>
        ///  Adds a contact point. Contact points are addresses of Cassandra nodes that
        ///  the driver uses to discover the cluster topology. Only one contact point is
        ///  required (the driver will retrieve the address of the other nodes
        ///  automatically), but it is usually a good idea to provide more than one
        ///  contact point, as if that unique contact point is not available, the driver
        ///  won't be able to initialize itself correctly.'
        /// </summary>
        /// <param name="address"> the address of the node to connect to </param>
        /// 
        /// <returns>this Builder </returns>
        public Builder AddContactPoint(string address)
        {
            _addresses.AddRange(Utils.ResolveHostByName(address));
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder </returns>
        public Builder AddContactPoints(params string[] addresses)
        {
            foreach (string address in addresses)
                AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(params IPAddress[] addresses)
        {
            foreach (IPAddress address in addresses)
                _addresses.Add(address);
            return this;
        }

        /// <summary>
        ///  Configure the load balancing policy to use for the new cluster. <p> If no
        ///  load balancing policy is set through this method,
        ///  <link>Policies.DefaultLoadBalancingPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the load balancing policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            _loadBalancingPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the reconnection policy to use for the new cluster. <p> If no
        ///  reconnection policy is set through this method,
        ///  <link>Policies.DefaultReconnectionPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the reconnection policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithReconnectionPolicy(IReconnectionPolicy policy)
        {
            _reconnectionPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the retry policy to use for the new cluster. <p> If no retry policy
        ///  is set through this method, <link>Policies.DefaultRetryPolicy</link> will
        ///  be used instead.</p>
        /// </summary>
        /// <param name="policy"> the retry policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithRetryPolicy(IRetryPolicy policy)
        {
            _retryPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the cluster by applying settings from ConnectionString. 
        /// </summary>
        /// <param name="connectionString"> the ConnectionString to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithConnectionString(string connectionString)
        {
            var cnb = new CassandraConnectionStringBuilder(connectionString);
            return cnb.ApplyToBuilder(this);
        }

        /// <summary>
        ///  Uses the provided credentials when connecting to Cassandra hosts. <p> This
        ///  should be used if the Cassandra cluster has been configured to use the
        ///  <c>PasswordAuthenticator</c>. If the the default <c>*
        ///  AllowAllAuthenticator</c> is used instead, using this method has no effect.</p>
        /// </summary>
        /// <param name="username"> the user name to use to login to Cassandra hosts.</param>
        /// <param name="password"> the password corresponding to </param>
        /// <returns>this Builder</returns>
        public Builder WithCredentials(String username, String password)
        {
            _authInfoProvider = new SimpleAuthInfoProvider().Add("username", username).Add("password", password);
            _authProvider = new PlainTextAuthProvider(username, password);
            return this;
        }


        /// <summary>
        ///  Use the specified AuthProvider when connecting to Cassandra hosts. <p> Use
        ///  this method when a custom authentication scheme is in place. You shouldn't
        ///  call both this method and {@code withCredentials}' on the same
        ///  <c>Builder</c> instance as one will supercede the other</p>
        /// </summary>
        /// <param name="authProvider"> the <link>AuthProvider"></link> to use to login to Cassandra hosts.</param>
        /// <returns>this Builder</returns>
        public Builder WithAuthProvider(IAuthProvider authProvider)
        {
            _authProvider = authProvider;
            return this;
        }

        /// <summary>
        ///  Disables row set buffering for the created cluster (row set buffering is enabled by
        ///  default otherwise).
        /// </summary>
        /// 
        /// <returns>this builder</returns>
        public Builder WithoutRowSetBuffering()
        {
            _withoutRowSetBuffering = true;
            return this;
        }


        /// <summary>
        ///  Sets the timeout for a single query within created cluster.
        ///  After the expiry of the timeout, query will be aborted.
        ///  Default timeout value is set to <c>Infinity</c>
        /// </summary>
        /// <param name="queryAbortTimeout">Timeout specified in milliseconds.</param>
        /// <returns>this builder</returns>
        public Builder WithQueryTimeout(int queryAbortTimeout)
        {
            _queryAbortTimeout = queryAbortTimeout;
            return this;
        }

        /// <summary>
        ///  Sets default keyspace name for the created cluster.
        /// </summary>
        /// <param name="defaultKeyspace">Default keyspace name.</param>
        /// <returns>this builder</returns>
        public Builder WithDefaultKeyspace(string defaultKeyspace)
        {
            _defaultKeyspace = defaultKeyspace;
            return this;
        }

        /// <summary>
        /// Configures the socket options that are going to be used to create the connections to the hosts.
        /// </summary>
        public Builder WithSocketOptions(SocketOptions value)
        {
            _socketOptions = value;
            return this;
        }

        public Builder WithPoolingOptions(PoolingOptions value)
        {
            _poolingOptions = value;
            return this;
        }

        /// <summary>
        ///  Enables the use of SSL for the created Cluster. Calling this method will use default SSL options. 
        /// </summary>
        /// <remarks>
        /// If SSL is enabled, the driver will not connect to any
        /// Cassandra nodes that doesn't have SSL enabled and it is strongly
        /// advised to enable SSL on every Cassandra node if you plan on using
        /// SSL in the driver. Note that SSL certificate common name(CN) on Cassandra node must match Cassandra node hostname.
        /// </remarks>
        /// <returns>this builder</returns>
        public Builder WithSSL()
        {
            _sslOptions = new SSLOptions();
            return this;
        }

        /// <summary>
        ///  Enables the use of SSL for the created Cluster using the provided options. 
        /// </summary>
        /// <remarks>
        /// If SSL is enabled, the driver will not connect to any
        /// Cassandra nodes that doesn't have SSL enabled and it is strongly
        /// advised to enable SSL on every Cassandra node if you plan on using
        /// SSL in the driver. Note that SSL certificate common name(CN) on Cassandra node must match Cassandra node hostname.
        /// </remarks>
        /// <param name="sslOptions">SSL options to use.</param>
        /// <returns>this builder</returns>        
        public Builder WithSSL(SSLOptions sslOptions)
        {
            _sslOptions = sslOptions;
            return this;
        }

        /// <summary>
        ///  Build the cluster with the configured set of initial contact points and
        ///  policies. This is a shorthand for <c>Cluster.buildFrom(this)</c>.
        /// </summary>
        /// 
        /// <returns>the newly build Cluster instance. </returns>
        public Cluster Build()
        {
            return Cluster.BuildFrom(this);
        }
    }
}