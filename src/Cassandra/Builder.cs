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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Cloud;
using Cassandra.DataStax.Graph;
using Cassandra.ExecutionProfiles;
using Cassandra.Metrics;
using Cassandra.Metrics.Abstractions;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    ///  Helper class to build <link>Cluster</link> instances.
    /// </summary>
    public class Builder : IInitializer
    {
        public const string DefaultApplicationName = "Default .NET Application";

        private static readonly Logger Logger = new Logger(typeof(Builder));

        private readonly List<object> _contactPoints = new List<object>();
        private const int DefaultQueryAbortTimeout = 20000;
        private PoolingOptions _poolingOptions;
        private SocketOptions _socketOptions = new SocketOptions();
        private IAuthInfoProvider _authInfoProvider;
        private IAuthProvider _authProvider = NoneAuthProvider.Instance;
        private CompressionType _compression = CompressionType.NoCompression;
        private IFrameCompressor _customCompressor;
        private string _defaultKeyspace;

        private ILoadBalancingPolicy _loadBalancingPolicy;
        private ITimestampGenerator _timestampGenerator;
        private int _port = ProtocolOptions.DefaultPort;
        private int _queryAbortTimeout = Builder.DefaultQueryAbortTimeout;
        private QueryOptions _queryOptions = new QueryOptions();
        private IReconnectionPolicy _reconnectionPolicy;
        private IRetryPolicy _retryPolicy;
        private SSLOptions _sslOptions;
        private bool _withoutRowSetBuffering;
        private IAddressTranslator _addressTranslator = new DefaultAddressTranslator();
        private ISpeculativeExecutionPolicy _speculativeExecutionPolicy;
        private ProtocolVersion _maxProtocolVersion = ProtocolVersion.MaxSupported;
        private TypeSerializerDefinitions _typeSerializerDefinitions;
        private bool _noCompact;
        private int _maxSchemaAgreementWaitSeconds = ProtocolOptions.DefaultMaxSchemaAgreementWaitSeconds;
        private IReadOnlyDictionary<string, IExecutionProfile> _profiles = new Dictionary<string, IExecutionProfile>();
        private MetadataSyncOptions _metadataSyncOptions;
        private IEndPointResolver _endPointResolver;
        private IDriverMetricsProvider _driverMetricsProvider;
        private DriverMetricsOptions _metricsOptions;
        private MonitorReportingOptions _monitorReportingOptions = new MonitorReportingOptions();
        private string _sessionName;
        private string _bundlePath;
        private bool _addedSsl;
        private bool _addedContactPoints;
        private bool _addedAuth;
        private bool _addedLbp;
        private bool? _keepContactPointsUnresolved;
        private bool? _allowBetaProtocolVersions;

        public Builder()
        {
        }

        internal Builder(ILoadBalancingPolicy lbp, IRetryPolicy retryPolicy)
        {
            // don't use With methods so that "_added*" flags are not set
            _loadBalancingPolicy = lbp;
            _retryPolicy = retryPolicy;
        }

        /// <summary>
        /// The version of the application using the created cluster instance.
        /// </summary>
        public string ApplicationVersion { get; private set; }

        /// <summary>
        /// The name of the application using the created cluster instance.
        /// </summary>
        public string ApplicationName { get; private set; }

        /// <summary>
        /// A unique identifier for the created cluster instance.
        /// </summary>
        public Guid? ClusterId { get; private set; }

        /// <summary>
        /// Gets the DSE Graph options.
        /// </summary>
        public GraphOptions GraphOptions { get; private set; }

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

        /// <summary>
        /// Gets the contact points that were added as <c>IPEndPoint"</c> instances.
        /// <para>
        /// Note that only contact points that were added using <see cref="AddContactPoint(IPEndPoint)"/> and
        /// <see cref="AddContactPoints(IPEndPoint[])"/> are returned by this property, as IP addresses and host names must be resolved and assigned
        /// the port number, which is performed on <see cref="Build()"/>.
        /// </para>
        /// </summary>
        public ICollection<IPEndPoint> ContactPoints
        {
            get { return _contactPoints.Select(c => c as IPEndPoint).Where(c => c != null).ToList(); }
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
            if (_bundlePath != null)
            {
                ConfigureCloudCluster(_bundlePath);
            }
            
            var typeSerializerDefinitions = _typeSerializerDefinitions ?? new TypeSerializerDefinitions();
            var policies = GetPolicies();
            var graphOptions = GetGraphOptions();
            SetLegacySettingsFromDefaultProfile();

            var protocolOptions =
                new ProtocolOptions(_port, _sslOptions)
                    .SetCompression(_compression)
                    .SetCustomCompressor(_customCompressor)
                    .SetMaxProtocolVersion(_maxProtocolVersion)
                    .SetNoCompact(_noCompact)
                    .SetMaxSchemaAgreementWaitSeconds(_maxSchemaAgreementWaitSeconds);

            var clientOptions = new ClientOptions(_withoutRowSetBuffering, _queryAbortTimeout, _defaultKeyspace);

            var config = new Configuration(
                policies,
                protocolOptions,
                _poolingOptions,
                _socketOptions,
                clientOptions,
                _authProvider,
                _authInfoProvider,
                _queryOptions,
                _addressTranslator,
                _profiles,
                _metadataSyncOptions,
                _endPointResolver,
                _driverMetricsProvider,
                _metricsOptions,
                _sessionName,
                graphOptions,
                ClusterId,
                ApplicationVersion,
                ApplicationName,
                _monitorReportingOptions,
                typeSerializerDefinitions,
                _keepContactPointsUnresolved,
                _allowBetaProtocolVersions);

            return config;
        }

        /// <summary>
        /// Initialize legacy properties with values provided by the default profile.
        /// Example: set SocketOptions.ReadTimeoutMillis from ExecutionProfile.ReadTimeoutMillis
        /// </summary>
        private void SetLegacySettingsFromDefaultProfile()
        {
            if (!_profiles.TryGetValue(Configuration.DefaultExecutionProfileName, out var profile))
            {
                return;
            }

            if (profile.ReadTimeoutMillis.HasValue)
            {
                _socketOptions.SetReadTimeoutMillis(profile.ReadTimeoutMillis.Value);
            }
                
            if (profile.ConsistencyLevel.HasValue)
            {
                _queryOptions.SetConsistencyLevel(profile.ConsistencyLevel.Value);
            }
                
            if (profile.SerialConsistencyLevel.HasValue)
            {
                _queryOptions.SetSerialConsistencyLevel(profile.SerialConsistencyLevel.Value);
            }
        }

        private GraphOptions GetGraphOptions()
        {
            var graphOptions = GraphOptions;

            if (_profiles.TryGetValue(Configuration.DefaultExecutionProfileName, out var profile))
            {
                graphOptions = profile.GraphOptions ?? graphOptions;
            }

            return graphOptions;
        }

        private Policies GetPolicies()
        {
            var lbp = _loadBalancingPolicy;
            var sep = _speculativeExecutionPolicy;
            var rep = _retryPolicy;

            if (!_profiles.TryGetValue(Configuration.DefaultExecutionProfileName, out var profile))
            {
                return new Policies(
                    lbp,
                    _reconnectionPolicy,
                    rep,
                    sep,
                    _timestampGenerator);
            }

            if (profile.LoadBalancingPolicy != null && _loadBalancingPolicy != null)
            {
                Builder.Logger.Warning(
                    "A load balancing policy was provided through the Builder.WithLoadBalancingPolicy method " +
                    "and another through the default execution profile. Policies provided through the default execution profile " +
                    "take precedence over policies specified through the Builder methods.");
            }
                
            if (profile.SpeculativeExecutionPolicy != null && _speculativeExecutionPolicy != null)
            {
                Builder.Logger.Warning(
                    "A speculative execution policy was provided through the Builder.WithSpeculativeExecutionPolicy method " +
                    "and another through the default execution profile. Policies provided through the default execution profile " +
                    "take precedence over policies specified through the Builder methods.");
            }
                
            if (profile.RetryPolicy != null && _retryPolicy != null)
            {
                Builder.Logger.Warning(
                    "A retry policy was provided through the Builder.WithRetryPolicy method " +
                    "and another through the default execution profile. Policies provided through the default execution profile " +
                    "take precedence over policies specified through the Builder methods.");
            }

            lbp = profile.LoadBalancingPolicy ?? lbp;
            sep = profile.SpeculativeExecutionPolicy ?? sep;
            rep = profile.RetryPolicy ?? rep;

            return new Policies(
                lbp,
                _reconnectionPolicy,
                rep,
                sep,
                _timestampGenerator);
        }

        /// <summary>
        /// <para>
        /// An optional configuration for providing a unique identifier for the created cluster instance.
        /// </para>
        /// If not provided, an id will generated.
        /// <para>
        /// This value is passed to the server as a startup option and is useful as metadata for describing a client connection.
        /// </para>
        /// </summary>
        /// <param name="id">The id to assign to this cluster instance.</param>
        /// <returns>this instance</returns>
        public Builder WithClusterId(Guid id)
        {
            ClusterId = id;
            return this;
        }

        /// <summary>
        /// <para>
        /// An optional configuration identifying the name of the application using this cluster instance.
        /// </para>
        /// This value is passed to the server as a startup option and is useful as metadata for describing a client connection.
        /// </summary>
        /// <param name="name">The name of the application using this cluster.</param>
        /// <returns>this instance</returns>
        public Builder WithApplicationName(string name)
        {
            ApplicationName = name ?? throw new ArgumentNullException(nameof(name));
            return this;
        }

        /// <summary>
        /// <para>
        /// An optional configuration identifying the version of the application using this cluster instance.
        /// </para>
        /// This value is passed to the server as a startup option and is useful as metadata for describing a client connection.
        /// </summary>
        /// <param name="version">The version of the application using this cluster.</param>
        /// <returns>this instance</returns>
        public Builder WithApplicationVersion(string version)
        {
            ApplicationVersion = version ?? throw new ArgumentNullException(nameof(version));
            return this;
        }

        /// <summary>
        /// Sets the DataStax Graph options.
        /// </summary>
        /// <returns>this instance</returns>
        public Builder WithGraphOptions(GraphOptions options)
        {
            GraphOptions = options;
            return this;
        }

        /// <summary>
        ///  The port to use to connect to all Cassandra hosts. If not set through this
        ///  method, the default port (9042) will be used instead.
        /// </summary>
        /// <param name="port"> the port to set. </param>
        /// <returns>this Builder</returns>
        public Builder WithPort(int port)
        {
            _port = port;
            foreach (var c in _contactPoints)
            {
                if (c is IPEndPoint ipEndPoint)
                {
                    ipEndPoint.Port = port;
                }
            }
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
        /// <returns>this Builder <see>ProtocolOptions.Compression</see></returns>
        public Builder WithCompression(CompressionType compression)
        {
            _compression = compression;
            return this;
        }

        /// <summary>
        /// Sets a custom compressor to be used for the compression type.
        /// If specified, the compression type is mandatory.
        /// If not specified the driver default compressor will be use for the compression type.
        /// </summary>
        /// <param name="compressor">Implementation of IFrameCompressor</param>
        public Builder WithCustomCompressor(IFrameCompressor compressor)
        {
            _customCompressor = compressor;
            return this;
        }

        /// <summary>
        ///  Adds a contact point. Contact points are addresses of Cassandra nodes that
        ///  the driver uses to discover the cluster topology. Only one contact point is
        ///  required (the driver will retrieve the address of the other nodes
        ///  automatically), but it is usually a good idea to provide more than one
        ///  contact point, as if that unique contact point is not available, the driver
        ///  won't be able to initialize itself correctly.
        /// </summary>
        /// <remarks>
        ///  However, this can be useful if the Cassandra nodes are behind a router and
        ///  are not accessed directly. Note that if you are in this situation
        ///  (Cassandra nodes are behind a router, not directly accessible), you almost
        ///  surely want to provide a specific <c>IAddressTranslator</c>
        ///  (through <link>Builder.WithAddressTranslater</link>) to translate actual
        ///  Cassandra node addresses to the addresses the driver should use, otherwise
        ///  the driver will not be able to auto-detect new nodes (and will generally not
        ///  function optimally).
        /// </remarks>
        /// <param name="address">the address of the node to connect to</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoint(string address)
        {
            return AddSingleContactPointInternal(address);
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoint(IPAddress address)
        {
            // Avoid creating IPEndPoint entries using the current port,
            // as the user might provide a different one by calling WithPort() after this call
            return AddSingleContactPointInternal(address);
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoint(IPEndPoint address)
        {
            return AddSingleContactPointInternal(address);
        }

        /// <summary>
        ///  Add contact points. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder </returns>
        public Builder AddContactPoints(params string[] addresses)
        {
            return AddMultipleContactPointsInternal(addresses.AsEnumerable());
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(IEnumerable<string> addresses)
        {
            return AddMultipleContactPointsInternal(addresses.AsEnumerable());
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(params IPAddress[] addresses)
        {
            return AddMultipleContactPointsInternal(addresses.AsEnumerable());
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(IEnumerable<IPAddress> addresses)
        {
            return AddMultipleContactPointsInternal(addresses);
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        ///
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(params IPEndPoint[] addresses)
        {
            return AddMultipleContactPointsInternal(addresses.AsEnumerable());
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        ///
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(IEnumerable<IPEndPoint> addresses)
        {
            return AddMultipleContactPointsInternal(addresses);
        }

        private Builder AddMultipleContactPointsInternal(IEnumerable<object> contactPoints)
        {
            if (contactPoints == null)
            {
                throw new ArgumentNullException(nameof(contactPoints));
            }

            _addedContactPoints = true;
            _contactPoints.AddRange(contactPoints);
            return this;
        }

        private Builder AddSingleContactPointInternal(object contactPoint)
        {
            if (contactPoint == null)
            {
                throw new ArgumentNullException(nameof(contactPoint));
            }

            _addedContactPoints = true;
            _contactPoints.Add(contactPoint);
            return this;
        }

        /// <summary>
        /// <para>
        /// Whether to resolve hostname based contact points every time the driver attempts to use it to open a connection.
        /// </para>
        /// <para>
        /// Note that not every contact point is usually added as a <see cref="Host"/> instance and only the <see cref="Host"/> instances
        /// can be picked by the <see cref="ILoadBalancingPolicy"/> as coordinators for application requests.
        /// The driver adds the node which is used for the control connection as a <see cref="Host"/> and then parses the remaining
        /// hosts and their IP addresses from system tables, therefore ignoring the remaining contact points unless the control connection
        /// needs to be reconnected.
        /// </para>
        /// </summary>
        /// <param name="keepContactPointsUnresolved"></param>
        /// <returns></returns>
        public Builder WithUnresolvedContactPoints(bool keepContactPointsUnresolved)
        {
            _keepContactPointsUnresolved = keepContactPointsUnresolved;
            return this;
        }

        /// <summary>
        /// Whether to allow beta protocol versions to be used. Do NOT set this in production environments as
        /// beta protocol versions are not supported or recommended for production use.
        /// </summary>
        public Builder WithBetaProtocolVersions()
        {
            _allowBetaProtocolVersions = true;
            return this;
        }

        /// <summary>
        /// <para>
        /// Configure the load balancing policy to use for the new cluster.
        /// </para>
        /// <para>
        /// If no load balancing policy is set through this method, <see cref="Policies.DefaultLoadBalancingPolicy"/> will be used instead.
        /// </para>
        /// <para>
        /// To specify the local datacenter with the default load balancing policy, use the following method to create a
        /// new policy instance: <see cref="Policies.NewDefaultLoadBalancingPolicy"/>.
        /// </para>
        /// </summary>
        /// <param name="policy"> the load balancing policy to use.</param>
        /// <returns>this Builder</returns>
        public Builder WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            _addedLbp = true;
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
        /// Configure the retry policy to be used for the new cluster.
        /// <para>
        /// When the retry policy is not set with this method, the <see cref="Policies.DefaultRetryPolicy" />
        /// will be used instead.
        /// </para>
        /// <para>
        /// Use a <see cref="IExtendedRetryPolicy"/> implementation to cover all error scenarios.
        /// </para>
        /// </summary>
        /// <param name="policy"> the retry policy to use </param>
        /// <returns>this Builder</returns>
        public Builder WithRetryPolicy(IRetryPolicy policy)
        {
            _retryPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the speculative execution to use for the new cluster.
        /// <para>
        /// If no speculative execution policy is set through this method, <see cref="Policies.DefaultSpeculativeExecutionPolicy"/> will be used instead.
        /// </para>
        /// </summary>
        /// <param name="policy"> the speculative execution policy to use </param>
        /// <returns>this Builder</returns>
        public Builder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy policy)
        {
            _speculativeExecutionPolicy = policy;
            return this;
        }

        /// <summary>
        /// Configures the generator that will produce the client-side timestamp sent with each query.
        /// <para>
        /// This feature is only available with protocol version 3 or above of the native protocol.
        /// With earlier versions, timestamps are always generated server-side, and setting a generator
        /// through this method will have no effect.
        /// </para>
        /// <para>
        /// If no generator is set through this method, the driver will default to client-side timestamps
        /// by using <see cref="AtomicMonotonicTimestampGenerator"/>.
        /// </para>
        /// </summary>
        /// <param name="generator">The generator to use.</param>
        /// <returns>This builder instance</returns>
        public Builder WithTimestampGenerator(ITimestampGenerator generator)
        {
            _timestampGenerator = generator;
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
            _addedAuth = true;
            _authInfoProvider = new SimpleAuthInfoProvider().Add("username", username).Add("password", password);
            _authProvider = new PlainTextAuthProvider(username, password);
            return this;
        }

        /// <summary>
        ///  Use the specified AuthProvider when connecting to Cassandra hosts. <p> Use
        ///  this method when a custom authentication scheme is in place. You shouldn't
        ///  call both this method and {@code withCredentials}' on the same
        ///  <c>Builder</c> instance as one will supersede the other</p>
        /// </summary>
        /// <param name="authProvider"> the <link>AuthProvider"></link> to use to login to Cassandra hosts.</param>
        /// <returns>this Builder</returns>
        public Builder WithAuthProvider(IAuthProvider authProvider)
        {
            _authProvider = authProvider ?? throw new ArgumentNullException(nameof(authProvider));
            _addedAuth = true;
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
        /// Specifies the number of milliseconds that the driver should wait for the response before the query times out in a synchronous operation.
        /// <para>
        /// This will cause that synchronous operations like <see cref="ISession.Execute(string)"/> to throw a <see cref="System.TimeoutException"/>
        /// after the specified number of milliseconds.
        /// </para>
        /// Default timeout value is set to <code>20,000</code> (20 seconds).
        /// </summary>
        /// <remarks>
        /// If you want to define a read timeout at a lower level, you can use <see cref="Cassandra.SocketOptions.SetReadTimeoutMillis(int)"/>.
        /// </remarks>
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
            _addedSsl = true;
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
            _addedSsl = true;
            _sslOptions = sslOptions;
            return this;
        }

        /// <summary>
        ///  Configures the address translator to use for the new cluster.
        /// </summary>
        /// <remarks>
        /// See <see cref="IAddressTranslator"/> for more detail on address translation,
        /// but the default translator, <see cref="DefaultAddressTranslator"/>, should be
        /// correct in most cases. If unsure, stick to the default.
        /// </remarks>
        /// <param name="addressTranslator">the translator to use.</param>
        /// <returns>this Builder</returns>
        public Builder WithAddressTranslator(IAddressTranslator addressTranslator)
        {
            _addressTranslator = addressTranslator;
            return this;
        }

        /// <summary>
        /// <para>Limits the maximum protocol version used to connect to the nodes</para>
        /// <para>
        /// When it is not set, the protocol version used is negotiated between the driver and the Cassandra
        /// cluster upon establishing the first connection.
        /// </para>
        /// <para>
        /// Useful when connecting to a cluster that contains nodes with different major/minor versions
        /// of Cassandra. For example, preparing for a rolling upgrade of the Cluster.
        /// </para>
        /// </summary>
        /// <param name="version">
        /// <para>The native protocol version.</para>
        /// <para>Different Cassandra versions support a range of protocol versions, for example: </para>
        /// <para>- Cassandra 2.0 (DSE 4.0 - 4.6): Supports protocol versions 1 and 2.</para>
        /// <para>- Cassandra 2.1 (DSE 4.7 - 4.8): Supports protocol versions 1, 2 and 3.</para>
        /// <para>- Cassandra 2.2: Supports protocol versions 1, 2, 3 and 4.</para>
        /// <para>- Cassandra 3.0: Supports protocol versions 3 and 4.</para>
        /// </param>
        /// <remarks>Some Cassandra features are only available with a specific protocol version.</remarks>
        /// <returns>this instance</returns>
        public Builder WithMaxProtocolVersion(byte version)
        {
            return WithMaxProtocolVersion((ProtocolVersion)version);
        }

        /// <summary>
        /// <para>Limits the maximum protocol version used to connect to the nodes</para>
        /// <para>
        /// When it is not set, the protocol version used is negotiated between the driver and the Cassandra
        /// cluster upon establishing the first connection.
        /// </para>
        /// <para>
        /// Useful when connecting to a cluster that contains nodes with different major/minor versions
        /// of Cassandra. For example, preparing for a rolling upgrade of the Cluster.
        /// </para>
        /// </summary>
        /// <remarks>Some Cassandra features are only available with a specific protocol version.</remarks>
        /// <returns>this instance</returns>
        public Builder WithMaxProtocolVersion(ProtocolVersion version)
        {
            if (version == 0)
            {
                throw new ArgumentException("Protocol version 0 does not exist.");
            }
            _maxProtocolVersion = version;
            return this;
        }

        /// <summary>
        /// Enables the NO_COMPACT startup option.
        /// <para>
        /// When this option is set, <c>SELECT</c>, <c>UPDATE</c>, <c>DELETE</c>, and <c>BATCH</c> statements
        /// on <c>COMPACT STORAGE</c> tables function in "compatibility" mode which allows seeing these tables
        /// as if they were "regular" CQL tables.
        /// </para>
        /// <para>
        /// This option only affects interactions with tables using <c>COMPACT STORAGE</c> and it is only
        /// supported by C* 3.0.16+, 3.11.2+, 4.0+ and DSE 6.0+.
        /// </para>
        /// </summary>
        public Builder WithNoCompact()
        {
            _noCompact = true;
            return this;
        }

        /// <summary>
        /// Sets the <see cref="TypeSerializer{T}"/> to be used, replacing the default ones.
        /// </summary>
        /// <param name="definitions"></param>
        /// <returns>this instance</returns>
        public Builder WithTypeSerializers(TypeSerializerDefinitions definitions)
        {
            if (_typeSerializerDefinitions != null)
            {
                const string message = "TypeSerializers definitions were already set." +
                    "Use a single TypeSerializerDefinitions instance and call Define() multiple times";
                throw new InvalidOperationException(message);
            }

            _typeSerializerDefinitions = definitions ?? throw new ArgumentNullException(nameof(definitions));
            return this;
        }
        
        internal Builder WithEndPointResolver(IEndPointResolver endPointResolver)
        {
            _endPointResolver = endPointResolver ?? throw new ArgumentNullException(nameof(endPointResolver));
            return this;
        }

        /// <summary>
        /// Sets the maximum time to wait for schema agreement before returning from a DDL query.
        /// <para/>
        /// DDL queries introduce schema changes that need to be propagated to all nodes in the cluster.
        /// Once they have settled on a common version, we say that they are in agreement.
        /// <para/>
        /// If not set through this method, the default value (10 seconds) will be used.
        /// </summary>
        /// <param name="maxSchemaAgreementWaitSeconds">The new value to set.</param>
        /// <returns>This Builder.</returns>
        /// <exception cref="ArgumentException">If the provided value is zero or less.</exception>
        public Builder WithMaxSchemaAgreementWaitSeconds(int maxSchemaAgreementWaitSeconds)
        {
            if (maxSchemaAgreementWaitSeconds <= 0)
            {
                throw new ArgumentException("Max schema agreement wait must be greater than zero");
            }

            _maxSchemaAgreementWaitSeconds = maxSchemaAgreementWaitSeconds;
            return this;
        }

        /// <summary>
        /// <para>
        /// Enables metrics. DataStax provides an implementation based on a third party library (App.Metrics)
        /// on a separate NuGet package: CassandraCSharpDriver.AppMetrics
        /// Alternatively, you can implement your own provider that implements <see cref="IDriverMetricsProvider"/>.
        /// </para>
        /// <para>
        /// This method enables all individual metrics without a bucket prefix. To customize these options,
        /// use <see cref="WithMetrics(IDriverMetricsProvider, DriverMetricsOptions)"/>.
        /// </para>
        /// </summary>
        /// <param name="driverMetricsProvider">Metrics Provider implementation.</param>
        /// <returns>This builder</returns>
        public Builder WithMetrics(IDriverMetricsProvider driverMetricsProvider)
        {
            _driverMetricsProvider = driverMetricsProvider ?? throw new ArgumentNullException(nameof(driverMetricsProvider));
            _metricsOptions = null;
            return this;
        }
        
        /// <summary>
        /// <para>
        /// Enables metrics. DataStax provides an implementation based on a third party library (App.Metrics)
        /// on a separate NuGet package: CassandraCSharpDriver.AppMetrics
        /// Alternatively, you can implement your own provider that implements <see cref="IDriverMetricsProvider"/>.
        /// </para>
        /// <para>
        /// This method enables all individual metrics without a bucket prefix. To customize these settings,
        /// use <see cref="WithMetrics(IDriverMetricsProvider, DriverMetricsOptions)"/>. For explanations on these settings,
        /// see the API docs of the <see cref="DriverMetricsOptions"/> class.
        /// </para> 
        /// <para>
        /// The AppMetrics provider also has some settings that can be customized, check out the API docs of
        /// Cassandra.AppMetrics.DriverAppMetricsOptions.
        /// <para>
        /// Here is an example:
        /// <code>
        /// var cluster = 
        ///     Cluster.Builder()
        ///            .WithMetrics(
        ///                metrics.CreateDriverMetricsProvider(new DriverAppMetricsOptions()),
        ///                new DriverMetricsOptions()
        ///                    .SetEnabledNodeMetrics(NodeMetric.DefaultNodeMetrics.Except(new [] { NodeMetric.Meters.BytesSent }))
        ///                    .SetEnabledSessionMetrics(
        ///                        SessionMetric.DefaultSessionMetrics.Except(new[] { SessionMetric.Meters.BytesReceived }))
        ///                    .SetBucketPrefix("web.app"))
        ///            .Build();
        /// </code>
        /// </para>
        /// </para>
        /// </summary>
        /// <param name="driverMetricsProvider">Metrics Provider implementation.</param>
        /// <param name="metricsOptions">Metrics Provider implementation.</param>
        /// <returns>This builder</returns>
        public Builder WithMetrics(IDriverMetricsProvider driverMetricsProvider, DriverMetricsOptions metricsOptions)
        {
            _driverMetricsProvider = driverMetricsProvider ?? throw new ArgumentNullException(nameof(driverMetricsProvider));
            _metricsOptions = metricsOptions?.Clone() ?? throw new ArgumentNullException(nameof(metricsOptions));
            return this;
        }

        /// <summary>
        /// <para>
        /// Adds Execution Profiles to the Cluster instance.
        /// </para>
        /// <para>
        /// Execution profiles are like configuration presets, multiple methods
        /// of the driver accept an execution profile name which is like telling the driver which settings to use for that particular request.
        /// This makes it easier to change settings like ConsistencyLevel and ReadTimeoutMillis on a per request basis.
        /// </para>
        /// <para>
        /// Note that subsequent calls to this method will override the previously provided profiles.
        /// </para>
        /// <para>
        /// To add execution profiles you can use
        /// <see cref="IExecutionProfileOptions.WithProfile(string,Action{IExecutionProfileBuilder})"/>:
        /// </para>
        /// <para>
        /// <code>
        ///         Cluster.Builder()
        ///                 .WithExecutionProfiles(options => options
        ///                     .WithProfile("profile1", profileBuilder => profileBuilder
        ///                         .WithReadTimeoutMillis(10000)
        ///                         .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)))
        ///                 .Build()
        /// </code>
        /// </para>
        /// </summary>
        /// <param name="profileOptionsBuilder"></param>
        /// <returns>This builder</returns>
        public Builder WithExecutionProfiles(Action<IExecutionProfileOptions> profileOptionsBuilder)
        {
            var profileOptions = new ExecutionProfileOptions();
            profileOptionsBuilder(profileOptions);
            _profiles = profileOptions.GetProfiles();
            return this;
        }

        /// <summary>
        /// <para>
        /// If not set through this method, the default value options will be used (metadata synchronization is enabled by default). The api reference of <see cref="MetadataSyncOptions"/>
        /// specifies what is the default for each option.
        /// </para>
        /// <para>
        /// In case you disable Metadata synchronization, please ensure you invoke <see cref="ICluster.RefreshSchemaAsync"/> in order to keep the token metadata up to date
        /// otherwise you will not be getting everything you can out of token aware routing, i.e. <see cref="TokenAwarePolicy"/>, which is enabled by the default.
        /// </para>
        /// <para>
        /// Disabling this feature has the following impact:
        ///
        /// <list type="bullet">
        ///
        /// <item><description>
        /// Token metadata will not be computed and stored.
        /// This means that token aware routing (<see cref="TokenAwarePolicy"/>, enabled by default) will only work correctly
        /// if you keep the token metadata up to date using the <see cref="ICluster.RefreshSchemaAsync"/> method.
        /// If you wish to go this route of manually refreshing the metadata then
        /// it's recommended to refresh only the keyspaces that this application will use, by passing the <code>keyspace</code> parameter.
        /// </description></item>
        ///
        /// <item><description>
        /// Keyspace metadata will not be cached by the driver. Every time you call methods like <see cref="Metadata.GetTable"/>, <see cref="Metadata.GetKeyspace"/>
        /// and other similar methods of the <see cref="Metadata"/> class, the driver will query that data on demand and will not cache it.
        /// </description></item>
        ///
        /// <item><description>
        /// The driver will not handle <code>SCHEMA_CHANGED</code> responses. This means that when you execute schema changing statements through the driver, it will
        /// not update the schema or topology metadata automatically before returning.
        /// </description></item>
        ///
        /// </list>
        /// </para>
        /// </summary>
        /// <summary>
        /// </summary>
        /// <param name="metadataSyncOptions">The new options to set.</param>
        /// <returns>This Builder.</returns>
        public Builder WithMetadataSyncOptions(MetadataSyncOptions metadataSyncOptions)
        {
            _metadataSyncOptions = metadataSyncOptions;
            return this;
        }
        
        /// <summary>
        /// <see cref="ISession"/> objects created through the <see cref="ICluster"/> built from this builder will have <see cref="ISession.SessionName"/>
        /// set to the value provided in this method.
        /// The first session created by this cluster instance will have its name set exactly as it is provided in this method.
        /// Any session created by the <see cref="ICluster"/> built from this builder after the first one will have its name set as a concatenation
        /// of the provided value plus a counter.
        /// <code>
        ///         var cluster = Cluster.Builder().WithSessionName("main-session").Build();
        ///         var session = cluster.Connect(); // session.SessionName == "main-session"
        ///         var session1 = cluster.Connect(); // session1.SessionName == "main-session1"
        ///         var session2 = cluster.Connect(); // session2.SessionName == "main-session2"
        /// </code>
        /// If this setting is not set, the default session names will be "s0", "s1", "s2", etc.
        /// <code>
        ///         var cluster = Cluster.Builder().Build();
        ///         var session = cluster.Connect(); // session.SessionName == "s0"
        ///         var session1 = cluster.Connect(); // session1.SessionName == "s1"
        ///         var session2 = cluster.Connect(); // session2.SessionName == "s2"
        /// </code>
        /// </summary>
        /// <param name="sessionName"></param>
        /// <returns></returns>
        public Builder WithSessionName(string sessionName)
        {
            _sessionName = sessionName ?? throw new ArgumentNullException(nameof(sessionName));
            return this;
        }

        /// <summary>
        /// <para>
        /// Configures a Cluster using the Cloud Secure Connection Bundle.
        /// Using this method will configure this builder with specific contact points, SSL options, credentials and load balancing policy.
        /// When needed, you can specify custom settings by calling other builder methods. 
        /// </para>
        /// <para>
        /// In case you need to specify a different set of credentials from the one in the bundle, here is an example:        /// <code>
        ///         Cluster.Builder()
        ///                   .WithCloudSecureConnectionBundle("/path/to/bundle.zip")
        ///                   .WithCredentials("username", "password")
        ///                   .Build();
        /// </code>
        /// </para> 
        /// <para>
        /// <see cref="Build"/> will throw <see cref="InvalidOperationException"/> when an error occurs that is not related to
        /// connectivity and <see cref="NoHostAvailableException"/> when an error occurs while trying to obtain the cluster metadata from the remote endpoint.
        /// </para>
        /// </summary>
        /// <param name="bundlePath">Path of the secure connection bundle.</param>
        /// <returns>A preconfigured builder ready for use.</returns>
        public Builder WithCloudSecureConnectionBundle(string bundlePath)
        {
            _bundlePath = bundlePath;
            return this;
        }

        /// <summary>
        /// Configures options related to Monitor Reporting for the new cluster.
        /// By default, Monitor Reporting is enabled for server types and versions that support it.
        /// </summary>
        /// <returns>This Builder.</returns>
        public Builder WithMonitorReporting(bool enabled)
        {
            return WithMonitorReporting(_monitorReportingOptions.SetMonitorReportingEnabled(enabled));
        }

        /// <summary>
        /// Configures options related to Monitor Reporting for the new cluster.
        /// By default, Monitor Reporting is enabled server types and versions that support it.
        /// </summary>
        /// <returns>This Builder.</returns>
        internal Builder WithMonitorReporting(MonitorReportingOptions options)
        {
            _monitorReportingOptions = options;
            return this;
        }

        /// <summary>
        ///  Build the cluster with the configured set of initial contact points and policies.
        /// </summary>
        /// <exception cref="NoHostAvailableException">Throws a NoHostAvailableException when no host could be resolved.</exception>
        /// <exception cref="ArgumentException">Throws an ArgumentException when no contact point was provided.</exception>
        /// <returns>the newly build Cluster instance. </returns>
        public Cluster Build()
        {
            // call GetConfiguration first in case it's a cloud cluster and this will set the contact points
            var config = GetConfiguration();

            return Cluster.BuildFrom(this, _contactPoints.Where(c => !(c is IPEndPoint)).ToList(), config);
        }
        
        /// <summary>
        /// Clear and set contact points.
        /// </summary>
        private Builder SetContactPoints(IEnumerable<object> contactPoints)
        {
            _contactPoints.Clear();
            return AddMultipleContactPointsInternal(contactPoints);
        }
        
        private Builder ConfigureCloudCluster(string bundlePath)
        {
            if (_addedSsl)
            {
                throw new ArgumentException("SSL options can not be set when a secure connection bundle is provided.");
            }
            
            if (_addedContactPoints)
            {
                throw new ArgumentException("Contact points can not be set when a secure connection bundle is provided.");
            }
            
            if (!_addedAuth)
            {
                throw new ArgumentException(
                    "No credentials were provided. When using the secure connection bundle, " +
                    "your cluster's credentials must be provided via the Builder.WithCredentials() method.");
            }

            SecureConnectionBundle bundle;
            try
            {
                bundle = new SecureConnectionBundleParser().ParseBundle(bundlePath);
            }
            catch (Exception ex2)
            {
                throw new InvalidOperationException(
                    "Failed to load or parse the secure connection bundle. See inner exception for more details.", ex2);
            }

            return ConfigureCloudCluster(bundle);
        }

        private Builder ConfigureCloudCluster(SecureConnectionBundle bundle)
        {
            var certificateValidator = new CustomCaCertificateValidator(bundle.CaCert, bundle.Config.Host);
            var sslOptions = new SSLOptions(
                SslProtocols.Tls12 | SslProtocols.Tls11 | SslProtocols.Tls,
                false,
                (sender, certificate, chain, errors) => certificateValidator.Validate(certificate, chain, errors));

            if (bundle.ClientCert != null)
            {
                sslOptions = sslOptions.SetCertificateCollection(new X509Certificate2Collection(new[] { bundle.ClientCert }));
            }

            var metadata = new CloudMetadataService();
            var clusterMetadata = Task.Run(
                () => metadata.GetClusterMetadataAsync(
                    $"https://{bundle.Config.Host}:{bundle.Config.Port}/metadata", 
                    this.SocketOptions,
                    sslOptions)).GetAwaiter().GetResult();

            var proxyAddress = clusterMetadata.ContactInfo.SniProxyAddress;
            var separatorIndex = proxyAddress.IndexOf(':');

            if (separatorIndex == -1)
            {
                throw new InvalidOperationException($"The SNI endpoint address should contain ip/name and port. Address received: {proxyAddress}");
            }

            var ipOrName = proxyAddress.Substring(0, separatorIndex);
            var port = int.Parse(proxyAddress.Substring(separatorIndex + 1));
            var isIp = IPAddress.TryParse(ipOrName, out var address);
            var sniOptions = new SniOptions(address, port, isIp ? null : ipOrName);
            
            var sniEndPointResolver = new SniEndPointResolver(new DnsResolver(), sniOptions);
            var builder = this.SetContactPoints(new List<object>
            {
                new SniContactPoint(new SortedSet<string>(clusterMetadata.ContactInfo.ContactPoints), sniEndPointResolver)
            });

            builder = builder.WithEndPointResolver(sniEndPointResolver);
            
            if (!_addedLbp)
            {
                if (clusterMetadata.ContactInfo.LocalDc == null)
                {
                    Builder.Logger.Warning("Not setting localDc property of DCAwareRoundRobinPolicy because the driver could not" +
                                                     "obtain it from the cluster metadata.");
                    builder = builder.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()));
                }
                else
                {
                    builder = builder.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(clusterMetadata.ContactInfo.LocalDc)));
                }
            }

            builder = builder.WithSSL(sslOptions)
                             .WithUnresolvedContactPoints(true);

            return builder;
        }
    }
}