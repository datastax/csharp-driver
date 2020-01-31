//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using Dse.Auth;
using Dse.Connections;
using Dse.ExecutionProfiles;
using Dse.Graph;
using Dse.Metrics;
using Dse.Metrics.Abstractions;
using Dse.Requests;
using Dse.Serialization;
using Dse.Serialization.Geometry;
using Dse.Serialization.Search;

namespace Dse
{
    /// <summary>
    /// Helper class to build <see cref="DseCluster"/> instances.
    /// </summary>
    public class DseClusterBuilder : Builder
    {
        public const string DefaultApplicationName = "Default .NET Application";

        private static readonly Logger Logger = new Logger(typeof(DseClusterBuilder));

        private readonly IDseCoreClusterFactory _dseCoreClusterFactory = new DseCoreClusterFactory();
        private TypeSerializerDefinitions _typeSerializerDefinitions;
        private IAddressTranslator _addressTranslator = new IdentityAddressTranslator();
        private MonitorReportingOptions _monitorReportingOptions = new MonitorReportingOptions();

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
        /// Creates a new instance of <see cref="DseClusterBuilder"/>.
        /// </summary>
        public DseClusterBuilder() 
            : base(DseLoadBalancingPolicy.CreateDefault(), new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()))
        {
        }

        /// <summary>
        /// <para>
        /// An optional configuration for providing a unique identifier for the created cluster instance.
        /// </para>
        /// If not provided, an id will generated.
        /// <para>
        /// This value is passed to DSE and is useful as metadata for describing a client connection.
        /// </para>
        /// </summary>
        /// <param name="id">The id to assign to this cluster instance.</param>
        /// <returns>this instance</returns>
        public DseClusterBuilder WithClusterId(Guid id)
        {
            ClusterId = id;
            return this;
        }

        /// <summary>
        /// <para>
        /// An optional configuration identifying the name of the application using this cluster instance.
        /// </para>
        /// This value is passed to DSE and is useful as metadata for describing a client connection.
        /// </summary>
        /// <param name="name">The name of the application using this cluster.</param>
        /// <returns>this instance</returns>
        public DseClusterBuilder WithApplicationName(string name)
        {
            ApplicationName = name ?? throw new ArgumentNullException(nameof(name));
            return this;
        }

        /// <summary>
        /// <para>
        /// An optional configuration identifying the version of the application using this cluster instance.
        /// </para>
        /// This value is passed to DSE and is useful as metadata for describing a client connection.
        /// </summary>
        /// <param name="version">The version of the application using this cluster.</param>
        /// <returns>this instance</returns>
        public DseClusterBuilder WithApplicationVersion(string version)
        {
            ApplicationVersion = version ?? throw new ArgumentNullException(nameof(version));
            return this;
        }

        /// <summary>
        /// Sets the DSE Graph options.
        /// </summary>
        /// <returns>this instance</returns>
        public DseClusterBuilder WithGraphOptions(GraphOptions options)
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
        public new DseClusterBuilder WithPort(int port)
        {
            base.WithPort(port);
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
        public new DseClusterBuilder WithQueryOptions(QueryOptions options)
        {
            base.WithQueryOptions(options);
            return this;
        }

        /// <summary>
        ///  Sets the compression to use for the transport.
        /// </summary>
        /// <param name="compression"> the compression to set </param>
        /// <returns>this Builder <see>ProtocolOptions.Compression</see></returns>
        public new DseClusterBuilder WithCompression(CompressionType compression)
        {
            base.WithCompression(compression);
            return this;
        }

        /// <summary>
        /// Sets a custom compressor to be used for the compression type.
        /// If specified, the compression type is mandatory.
        /// If not specified the driver default compressor will be use for the compression type.
        /// </summary>
        /// <param name="compressor">Implementation of IFrameCompressor</param>
        public new DseClusterBuilder WithCustomCompressor(IFrameCompressor compressor)
        {
            base.WithCustomCompressor(compressor);
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
        public new DseClusterBuilder AddContactPoint(string address)
        {
            base.AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoint(IPAddress address)
        {
            base.AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoint(IPEndPoint address)
        {
            base.AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder </returns>
        public new DseClusterBuilder AddContactPoints(params string[] addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(IEnumerable<string> addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(params IPAddress[] addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(IEnumerable<IPAddress> addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(params IPEndPoint[] addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        /// </param>
        /// <returns>this instance</returns>
        public new DseClusterBuilder AddContactPoints(IEnumerable<IPEndPoint> addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        /// Configures the load balancing policy to use for the new cluster.
        /// <para>
        /// If no load balancing policy is set through this method, <see cref="DseLoadBalancingPolicy"/>
        /// will be used instead.
        /// </para>
        /// <para>
        /// To specify the local datacenter, use the following method <see cref="DseLoadBalancingPolicy.CreateDefault(string)"/>
        /// to create an instance of the default policy with a specific local datacenter.
        /// </para>
        /// </summary>
        /// <param name="policy"> the load balancing policy to use </param>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            base.WithLoadBalancingPolicy(policy);
            return this;
        }

        /// <summary>
        ///  Configure the reconnection policy to use for the new cluster. <p> If no
        ///  reconnection policy is set through this method,
        ///  <link>Policies.DefaultReconnectionPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the reconnection policy to use </param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithReconnectionPolicy(IReconnectionPolicy policy)
        {
            base.WithReconnectionPolicy(policy);
            return this;
        }

        /// <summary>
        /// Configure the retry policy to use for the new cluster.
        /// <para>
        /// If no retry policy is set through this method, <see cref="IdempotenceAwareRetryPolicy"/> will be
        /// used instead.
        /// </para>
        /// </summary>
        /// <param name="policy"> the retry policy to use </param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithRetryPolicy(IRetryPolicy policy)
        {
            base.WithRetryPolicy(policy);
            return this;
        }

        /// <summary>
        ///  Configure the speculative execution to use for the new cluster.
        /// <para>
        /// If no speculative execution policy is set through this method, <see cref="Dse.Policies.DefaultSpeculativeExecutionPolicy"/> will be used instead.
        /// </para>
        /// </summary>
        /// <param name="policy"> the speculative execution policy to use </param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy policy)
        {
            base.WithSpeculativeExecutionPolicy(policy);
            return this;
        }

        /// <summary>
        ///  Configure the cluster by applying settings from ConnectionString.
        /// </summary>
        /// <param name="connectionString"> the ConnectionString to use </param>
        ///
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithConnectionString(string connectionString)
        {
            base.WithConnectionString(connectionString);
            return this;
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
        public new DseClusterBuilder WithCredentials(String username, String password)
        {
            this.WithAuthProvider(new DsePlainTextAuthProvider(username, password));
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
        public new DseClusterBuilder WithAuthProvider(IAuthProvider authProvider)
        {
            base.WithAuthProvider(authProvider);
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
        /// If you want to define a read timeout at a lower level, you can use <see cref="SocketOptions.SetReadTimeoutMillis(int)"/>.
        /// </remarks>
        /// <param name="queryAbortTimeout">Timeout specified in milliseconds.</param>
        /// <returns>this builder</returns>
        public new DseClusterBuilder WithQueryTimeout(int queryAbortTimeout)
        {
            base.WithQueryTimeout(queryAbortTimeout);
            return this;
        }

        /// <summary>
        ///  Sets default keyspace name for the created cluster.
        /// </summary>
        /// <param name="defaultKeyspace">Default keyspace name.</param>
        /// <returns>this builder</returns>
        public new DseClusterBuilder WithDefaultKeyspace(string defaultKeyspace)
        {
            base.WithDefaultKeyspace(defaultKeyspace);
            return this;
        }

        /// <summary>
        /// Configures the socket options that are going to be used to create the connections to the hosts.
        /// </summary>
        public new DseClusterBuilder WithSocketOptions(SocketOptions value)
        {
            base.WithSocketOptions(value);
            return this;
        }

        /// <summary>
        /// Sets the pooling options for the cluster.
        /// </summary>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithPoolingOptions(PoolingOptions value)
        {
            base.WithPoolingOptions(value);
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
        // ReSharper disable once InconsistentNaming
        public new DseClusterBuilder WithSSL()
        {
            base.WithSSL();
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
        // ReSharper disable once InconsistentNaming
        public new DseClusterBuilder WithSSL(SSLOptions sslOptions)
        {
            base.WithSSL(sslOptions);
            return this;
        }

        /// <summary>
        ///  Configures the address translater to use for the new cluster.
        /// </summary>
        /// <remarks>
        /// See <c>IAddressTranslater</c> for more detail on address translation,
        /// but the default tanslater, <c>DefaultAddressTranslator</c>, should be
        /// correct in most cases. If unsure, stick to the default.
        /// </remarks>
        /// <param name="addressTranslator">the translater to use.</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithAddressTranslator(IAddressTranslator addressTranslator)
        {
            _addressTranslator = addressTranslator;
            base.WithAddressTranslator(addressTranslator);
            return this;
        }

        /// <summary>
        /// <para>Limits the maximum protocol version used to connect to the nodes, when it is not set
        /// protocol version used between the driver and the Cassandra cluster is negotiated upon establishing
        /// the first connection.</para>
        /// <para>Useful for using the driver against a cluster that contains nodes with different major/minor versions
        /// of Cassandra. For example, preparing for a rolling upgrade of the Cluster.</para>
        /// </summary>
        /// <param name="version">
        /// <para>The native protocol version.</para>
        /// <para>Different Cassandra versions support a range of protocol versions, for example: </para>
        /// <para>- Cassandra 2.0 (DSE 4.0 – 4.6): Supports protocol versions 1 and 2.</para>
        /// <para>- Cassandra 2.1 (DSE 4.7 – 4.8): Supports protocol versions 1, 2 and 3.</para>
        /// <para>- Cassandra 2.2: Supports protocol versions 1, 2, 3 and 4.</para>
        /// <para>- Cassandra 3.0: Supports protocol versions 3 and 4.</para>
        /// </param>
        /// <remarks>Some Cassandra features are only available with a specific protocol version.</remarks>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithMaxProtocolVersion(byte version)
        {
            base.WithMaxProtocolVersion(version);
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
        /// supported by DSE 5.1 and 6.0+.
        /// </para>
        /// </summary>
        public new DseClusterBuilder WithNoCompact()
        {
            base.WithNoCompact();
            return this;
        }

        /// <summary>
        /// Sets the <see cref="TypeSerializer{T}"/> to be used, replacing the default ones.
        /// </summary>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithTypeSerializers(TypeSerializerDefinitions definitions)
        {
            //Store the definitions
            //If the definitions for GeoTypes or other have already been defined those will be considered.
            _typeSerializerDefinitions = definitions;
            return this;
        }

        /// <summary>
        /// Configures options related to Monitor Reporting for the new cluster.
        /// By default, Monitor Reporting is enabled.
        /// </summary>
        /// <returns>This Builder.</returns>
        public DseClusterBuilder WithMonitorReporting(bool enabled)
        {
            return WithMonitorReporting(_monitorReportingOptions.SetMonitorReportingEnabled(enabled));
        }

        /// <summary>
        /// Configures options related to Monitor Reporting for the new cluster.
        /// By default, Monitor Reporting is enabled.
        /// </summary>
        /// <returns>This Builder.</returns>
        internal DseClusterBuilder WithMonitorReporting(MonitorReportingOptions options)
        {
            _monitorReportingOptions = options;
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
        public new DseClusterBuilder WithMaxSchemaAgreementWaitSeconds(int maxSchemaAgreementWaitSeconds)
        {
            base.WithMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);
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
        public new DseClusterBuilder WithTimestampGenerator(ITimestampGenerator generator)
        {
            base.WithTimestampGenerator(generator);
            return this;
        }

        /// <summary>
        /// <para>
        /// Adds Execution Profiles to the DseCluster instance.
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
        ///         DseCluster.Builder()
        ///                 .WithExecutionProfiles(options => options
        ///                     .WithProfile("profile1", profileBuilder => profileBuilder
        ///                         .WithReadTimeoutMillis(10000)
        ///                         .WithConsistencyLevel(ConsistencyLevel.LocalQuorum))
        ///                     .WithProfile("profile-graph", profileBuilder => profileBuilder
        ///                         .WithReadTimeoutMillis(10000)
        ///                         .WithGraphOptions(new GraphOptions().SetName("name"))))
        ///                 .Build();
        /// </code>
        /// </para>
        /// </summary>
        /// <param name="profileOptionsBuilder"></param>
        /// <returns>This builder</returns>
        public new DseClusterBuilder WithExecutionProfiles(Action<IExecutionProfileOptions> profileOptionsBuilder)
        {
            base.WithExecutionProfiles(profileOptionsBuilder);
            return this;
        }

        /// <summary>
        /// <para>
        /// If not set through this method, the default value options will be used (metadata synchronization is enabled by default). The api reference of <see cref="MetadataSyncOptions"/>
        /// specifies what is the default for each option.
        /// </para>
        /// <para>
        /// In case you disable Metadata synchronization, please ensure you invoke <see cref="ICluster.RefreshSchemaAsync"/> in order to keep the token metadata up to date
        /// otherwise you will not be getting everything you can out of token aware routing, i.e. <see cref="TokenAwarePolicy"/>, which is used by <see cref="DseLoadBalancingPolicy"/> and is the default. 
        /// </para>
        /// <para>
        /// Disabling this feature has the following impact:
        /// 
        /// <list type="bullet">
        /// 
        /// <item><description>
        /// Token metadata will not be computed and stored.
        /// This means that token aware routing (<see cref="TokenAwarePolicy"/>, which is used by <see cref="DseLoadBalancingPolicy"/> and is the default) will only work correctly
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
        public new DseClusterBuilder WithMetadataSyncOptions(MetadataSyncOptions metadataSyncOptions)
        {
            base.WithMetadataSyncOptions(metadataSyncOptions);
            return this;
        }
        
        /// <summary>
        /// <para>
        /// Enables metrics. DataStax provides an implementation based on a third party library (App.Metrics)
        /// on a separate NuGet package: Dse.AppMetrics
        /// Alternatively, you can implement your own provider that implements <see cref="IDriverMetricsProvider"/>.
        /// </para>
        /// <para>
        /// This method enables all individual metrics without a bucket prefix. To customize these options,
        /// use <see cref="WithMetrics(IDriverMetricsProvider, DriverMetricsOptions)"/>.
        /// </para>
        /// </summary>
        /// <param name="driverMetricsProvider">Metrics Provider implementation.</param>
        /// <returns>This builder</returns>
        public new DseClusterBuilder WithMetrics(IDriverMetricsProvider driverMetricsProvider)
        {
            base.WithMetrics(driverMetricsProvider);
            return this;
        }
        
        /// <summary>
        /// <para>
        /// Enables metrics. DataStax provides an implementation based on a third party library (App.Metrics)
        /// on a separate NuGet package: Dse.AppMetrics
        /// Alternatively, you can implement your own provider that implements <see cref="IDriverMetricsProvider"/>.
        /// </para>
        /// <para>
        /// This method enables all individual metrics without a bucket prefix. To customize these settings,
        /// use <see cref="WithMetrics(IDriverMetricsProvider, DriverMetricsOptions)"/>. For explanations on these settings,
        /// see the API docs of the <see cref="DriverMetricsOptions"/> class.
        /// </para> 
        /// <para>
        /// The AppMetrics provider also has some settings that can be customized, check out the API docs of
        /// Dse.AppMetrics.DriverAppMetricsOptions.
        /// <para>
        /// Here is an example:
        /// <code>
        /// var cluster = 
        ///     DseCluster.Builder()
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
        public new DseClusterBuilder WithMetrics(IDriverMetricsProvider driverMetricsProvider, DriverMetricsOptions metricsOptions)
        {
            base.WithMetrics(driverMetricsProvider, metricsOptions);
            return this;
        }
        
        /// <summary>
        /// <see cref="IDseSession"/> objects created through the <see cref="IDseCluster"/> built from this builder will have <see cref="ISession.SessionName"/>
        /// set to the value provided in this method.
        /// The first session created by this cluster instance will have its name set exactly as it is provided in this method.
        /// Any session created by the <see cref="IDseCluster"/> built from this builder after the first one will have its name set as a concatenation
        /// of the provided value plus a counter.
        /// <code>
        ///         var cluster = DseCluster.Builder().WithSessionName("main-session").Build();
        ///         var session = cluster.Connect(); // session.SessionName == "main-session"
        ///         var session1 = cluster.Connect(); // session1.SessionName == "main-session1"
        ///         var session2 = cluster.Connect(); // session2.SessionName == "main-session2"
        /// </code>
        /// If this setting is not set, the default session names will be "s0", "s1", "s2", etc.
        /// <code>
        ///         var cluster = DseCluster.Builder().Build();
        ///         var session = cluster.Connect(); // session.SessionName == "s0"
        ///         var session1 = cluster.Connect(); // session1.SessionName == "s1"
        ///         var session2 = cluster.Connect(); // session2.SessionName == "s2"
        /// </code>
        /// </summary>
        /// <param name="sessionName"></param>
        /// <returns></returns>
        public new DseClusterBuilder WithSessionName(string sessionName)
        {
            base.WithSessionName(sessionName);
            return this;
        }

        /// <summary>
        /// <para>
        /// Configures a DseCluster using the Cloud Secure Connection Bundle.
        /// Using this method will configure this builder with specific contact points, SSL options, credentials and load balancing policy.
        /// When needed, you can specify custom settings by calling other builder methods. 
        /// </para>
        /// <para>
        /// In case you need to specify a different set of credentials from the one in the bundle, here is an example:
        /// <code>
        ///         DseCluster.Builder()
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
        public new DseClusterBuilder WithCloudSecureConnectionBundle(string bundlePath)
        {
            base.WithCloudSecureConnectionBundle(bundlePath);
            return this;
        }

        internal new DseClusterBuilder WithEndPointResolver(IEndPointResolver endPointResolver)
        {
            base.WithEndPointResolver(endPointResolver);
            return this;
        }

        /// <summary>
        /// Builds the cluster with the configured set of initial contact points and policies.
        /// </summary>
        /// <returns>
        /// A new <see cref="DseCluster"/> instance.
        /// </returns>
        public new DseCluster Build()
        {
            var dseAssembly = typeof(DseCluster).GetTypeInfo().Assembly;
            DseClusterBuilder.Logger.Info(
                "Using DataStax C# DSE driver v{0}",
                FileVersionInfo.GetVersionInfo(dseAssembly.Location).FileVersion);

            var typeSerializerDefinitions = _typeSerializerDefinitions ?? new TypeSerializerDefinitions();
            typeSerializerDefinitions
                .Define(new DateRangeSerializer())
                .Define(new DurationSerializer(true))
                .Define(new LineStringSerializer())
                .Define(new PointSerializer())
                .Define(new PolygonSerializer());

            var clusterId = ClusterId ?? Guid.NewGuid();
            var appVersion = ApplicationVersion ?? DseConfiguration.DefaultApplicationVersion;
            var appName = ApplicationName ?? DseConfiguration.FallbackApplicationName;
            var graphOptions = GetGraphOptions() ?? new GraphOptions();

            base.WithTypeSerializers(typeSerializerDefinitions);
            base.WithStartupOptionsFactory(new DseStartupOptionsFactory(clusterId, appVersion, appName));
            base.WithRequestOptionsMapper(new RequestOptionsMapper(graphOptions));
            var cassandraConfig = base.GetConfiguration();

            var config = new DseConfiguration(
                cassandraConfig,
                graphOptions,
                clusterId,
                appVersion,
                appName,
                _monitorReportingOptions,
                _addressTranslator,
                DseConfiguration.DefaultInsightsSupportVerifier,
                DseConfiguration.DefaultDseSessionManagerFactory,
                DseConfiguration.GetDefaultDseSessionFactoryBuilder(cassandraConfig.SessionFactoryBuilder));

            return new DseCluster(this, HostNames, config, _dseCoreClusterFactory);
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
    }
}