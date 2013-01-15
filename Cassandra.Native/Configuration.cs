namespace Cassandra
{

    /// <summary>
    ///  The configuration of the cluster. This handle setting: <ul> <li>Cassandra
    ///  binary protocol level configuration (compression).</li> <li>Connection
    ///  pooling configurations.</li> <li>low-level tcp configuration options
    ///  (tcpNoDelay, keepAlive, ...).</li> </ul>
    /// </summary>
    public class Configuration
    {

        private readonly Policies _policies;

        private readonly ProtocolOptions _protocolOptions;
        private readonly PoolingOptions _poolingOptions;
        private readonly SocketOptions _socketOptions;
        private readonly ClientOptions _clientOptions;

        private readonly IAuthInfoProvider _authProvider;
        private readonly bool _metricsEnabled;

        public Configuration() :
            this(new Policies(),
                 new ProtocolOptions(),
                 new PoolingOptions(),
                 new SocketOptions(),
                 new ClientOptions(),
                 null,
                 true)
        {
        }

        public Configuration(Policies policies,
                             ProtocolOptions protocolOptions,
                             PoolingOptions poolingOptions,
                             SocketOptions socketOptions,
                             ClientOptions clientOptions,
                             IAuthInfoProvider authProvider,
                             bool metricsEnabled)
        {
            this._policies = policies;
            this._protocolOptions = protocolOptions;
            this._poolingOptions = poolingOptions;
            this._socketOptions = socketOptions;
            this._clientOptions = clientOptions;
            this._authProvider = authProvider;
            this._metricsEnabled = metricsEnabled;
        }

        /// <summary>
        ///  Gets the policies set for the cluster.
        /// </summary>
        public Policies Policies
        {
            get {return _policies;}
        }

        /// <summary>
        ///  Gets the low-level tcp configuration options used (tcpNoDelay, keepAlive, ...).
        /// </summary>
        public SocketOptions SocketOptions
        {
            get {return _socketOptions;}
        }

        /// <summary>
        ///  The Cassandra binary protocol level configuration (compression).
        /// </summary>
        /// 
        /// <returns>the protocol options.</returns>
        public ProtocolOptions ProtocolOptions
        {
             get {return _protocolOptions;}
        }

        /// <summary>
        ///  The connection pooling configuration.
        /// </summary>
        /// 
        /// <returns>the pooling options.</returns>
        public PoolingOptions PoolingOptions
        {
            get {return _poolingOptions;}
        }

        /// <summary>
        ///  The .net client additional options configuration.
        /// </summary>
        public ClientOptions ClientOptions
        {
            get { return _clientOptions; }
        }

        /// <summary>
        ///  The authentication provider used to connect to the Cassandra cluster.
        /// </summary>
        /// 
        /// <returns>the authentication provider in use.</returns>
        public IAuthInfoProvider AuthInfoProvider
        {
            get { return _authProvider; }
        }

        /// <summary>
        ///  Whether metrics collection is enabled for the cluster instance. <p> Metrics
        ///  collection is enabled by default but can be disabled at cluster construction
        ///  time through <link>Cluster.Builder#withoutMetrics</link>.
        /// </summary>
        /// 
        /// <returns>whether metrics collection is enabled for the cluster
        ///  instance.</returns>

        public bool MetricsEnabled
        {
            get { return _metricsEnabled; }
        }

    }
}

// end namespace