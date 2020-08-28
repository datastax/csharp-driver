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

using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Insights;
using Cassandra.DataStax.Insights.InfoProviders;
using Cassandra.DataStax.Insights.InfoProviders.StartupMessage;
using Cassandra.DataStax.Insights.InfoProviders.StatusMessage;
using Cassandra.DataStax.Insights.MessageFactories;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.DataStax.Insights.Schema.StatusMessage;
using Cassandra.ExecutionProfiles;
using Cassandra.Helpers;
using Cassandra.MetadataHelpers;
using Cassandra.Metrics;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Observers;
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

using Microsoft.IO;

namespace Cassandra
{
    /// <summary>
    ///  The configuration of the cluster. It configures the following: <ul> <li>Cassandra
    ///  binary protocol level configuration (compression).</li> <li>Connection
    ///  pooling configurations.</li> <li>low-level tcp configuration options
    ///  (tcpNoDelay, keepAlive, ...).</li> </ul>
    /// </summary>
    public class Configuration
    {
        internal const string DefaultExecutionProfileName = "default";
        internal const string DefaultSessionName = "s";

        /// <summary>
        ///  Gets the policies set for the cluster.
        /// </summary>
        public Policies Policies { get; }

        /// <summary>
        ///  Gets the low-level tcp configuration options used (tcpNoDelay, keepAlive, ...).
        /// </summary>
        public SocketOptions SocketOptions { get; private set; }

        /// <summary>
        ///  The Cassandra binary protocol level configuration (compression).
        /// </summary>
        ///
        /// <returns>the protocol options.</returns>
        public ProtocolOptions ProtocolOptions { get; private set; }

        /// <summary>
        ///  The connection pooling configuration, defaults to null.
        /// </summary>
        /// <returns>the pooling options.</returns>
        public PoolingOptions PoolingOptions { get; }

        /// <summary>
        ///  The .net client additional options configuration.
        /// </summary>
        public ClientOptions ClientOptions { get; private set; }

        /// <summary>
        ///  The query configuration.
        /// </summary>
        public QueryOptions QueryOptions { get; private set; }

        /// <summary>
        ///  The authentication provider used to connect to the Cassandra cluster.
        /// </summary>
        /// <returns>the authentication provider in use.</returns>
        internal IAuthProvider AuthProvider { get; private set; } // Not exposed yet on purpose

        /// <summary>
        ///  The authentication provider used to connect to the Cassandra cluster.
        /// </summary>
        /// <returns>the authentication provider in use.</returns>
        internal IAuthInfoProvider AuthInfoProvider { get; private set; } // Not exposed yet on purpose

        /// <summary>
        ///  The address translator used to translate Cassandra node address.
        /// </summary>
        /// <returns>the address translator in use.</returns>
        public IAddressTranslator AddressTranslator { get; private set; }

        /// <summary>
        /// Gets a read only key value map of execution profiles that were configured with
        /// <see cref="Builder.WithExecutionProfiles"/>. The keys are execution profile names and the values
        /// are <see cref="IExecutionProfile"/> instances.
        /// </summary>
        public IReadOnlyDictionary<string, IExecutionProfile> ExecutionProfiles { get; }

        /// <summary>
        /// <see cref="Builder.WithUnresolvedContactPoints"/>
        /// </summary>
        public bool KeepContactPointsUnresolved { get; }

        /// <summary>
        /// Shared reusable timer
        /// </summary>
        internal HashedWheelTimer Timer { get; private set; }

        /// <summary>
        /// Shared buffer pool
        /// </summary>
        internal RecyclableMemoryStreamManager BufferPool { get; private set; }

        /// <summary>
        /// Gets or sets the list of <see cref="TypeSerializer{T}"/> defined.
        /// </summary>
        internal IEnumerable<ITypeSerializer> TypeSerializers { get; set; }

        internal MetadataSyncOptions MetadataSyncOptions { get; }

        internal IStartupOptionsFactory StartupOptionsFactory { get; }

        internal ISessionFactory SessionFactory { get; }

        internal IRequestOptionsMapper RequestOptionsMapper { get; }

        internal IRequestHandlerFactory RequestHandlerFactory { get; }

        internal IHostConnectionPoolFactory HostConnectionPoolFactory { get; }

        internal IRequestExecutionFactory RequestExecutionFactory { get; }

        internal IConnectionFactory ConnectionFactory { get; }

        internal IControlConnectionFactory ControlConnectionFactory { get; }

        internal IPrepareHandlerFactory PrepareHandlerFactory { get; }

        internal ITimerFactory TimerFactory { get; }

        internal IEndPointResolver EndPointResolver { get; }

        internal IDnsResolver DnsResolver { get; }

        internal IMetadataRequestHandler MetadataRequestHandler { get; }

        internal ITopologyRefresherFactory TopologyRefresherFactory { get; }

        internal ISchemaParserFactory SchemaParserFactory { get; }

        internal ISupportedOptionsInitializerFactory SupportedOptionsInitializerFactory { get; }

        internal IProtocolVersionNegotiator ProtocolVersionNegotiator { get; }

        internal IServerEventsSubscriber ServerEventsSubscriber { get; }

        internal IDriverMetricsProvider MetricsProvider { get; }

        internal DriverMetricsOptions MetricsOptions { get; }

        internal string SessionName { get; }

        internal bool MetricsEnabled { get; }

        internal IObserverFactoryBuilder ObserverFactoryBuilder { get; }
        
        internal static string DefaultApplicationVersion => string.Empty;

        internal static string FallbackApplicationName =>
            AssemblyHelpers.GetEntryAssembly()?.GetName().Name ?? Builder.DefaultApplicationName;

        /// <summary>
        /// The version of the application using the created cluster instance.
        /// </summary>
        public string ApplicationVersion { get; }

        /// <summary>
        /// The name of the application using the created cluster instance.
        /// </summary>
        public string ApplicationName { get; }

        /// <summary>
        /// Specifies whether <see cref="ApplicationName"/> was generated by the driver.
        /// </summary>
        public bool ApplicationNameWasGenerated { get; }

        /// <summary>
        /// A unique identifier for the created cluster instance.
        /// </summary>
        public Guid ClusterId { get; }
        
        /// <summary>
        /// Gets the options related to graph instance.
        /// </summary>
        public GraphOptions GraphOptions { get; protected set; }

        /// <summary>
        /// Whether beta protocol versions will be considered by the driver during
        /// the protocol version negotiation.
        /// </summary>
        public bool AllowBetaProtocolVersions { get; }

        /// <summary>
        /// The key is the execution profile name and the value is the IRequestOptions instance
        /// built from the execution profile with that key.
        /// </summary>
        internal IReadOnlyDictionary<string, IRequestOptions> RequestOptions { get; }

        /// <summary>
        /// Configuration options for monitor reporting
        /// </summary>
        public MonitorReportingOptions MonitorReportingOptions { get; }

        internal IInsightsSupportVerifier InsightsSupportVerifier { get; }

        internal IInsightsClientFactory InsightsClientFactory { get; }

        internal IRequestOptions DefaultRequestOptions => RequestOptions[Configuration.DefaultExecutionProfileName];
        
        internal static IInsightsSupportVerifier DefaultInsightsSupportVerifier => new InsightsSupportVerifier();
        
        internal static IInsightsClientFactory DefaultInsightsClientFactory =>
            new InsightsClientFactory(
                Configuration.DefaultInsightsStartupMessageFactory, Configuration.DefaultInsightsStatusMessageFactory);

        internal static IInsightsMessageFactory<InsightsStartupData> DefaultInsightsStartupMessageFactory =>
            new InsightsStartupMessageFactory(
                Configuration.DefaultInsightsMetadataFactory,
                Configuration.DefaultInsightsInfoProvidersCollection
            );

        internal static IInsightsMessageFactory<InsightsStatusData> DefaultInsightsStatusMessageFactory =>
            new InsightsStatusMessageFactory(
                Configuration.DefaultInsightsMetadataFactory,
                new NodeStatusInfoProvider()
            );

        internal static IInsightsMetadataFactory DefaultInsightsMetadataFactory =>
            new InsightsMetadataFactory(new InsightsMetadataTimestampGenerator());

        internal static InsightsInfoProvidersCollection DefaultInsightsInfoProvidersCollection =>
            new InsightsInfoProvidersCollection(
                new PlatformInfoProvider(),
                new ExecutionProfileInfoProvider(
                    new LoadBalancingPolicyInfoProvider(new ReconnectionPolicyInfoProvider()),
                    new SpeculativeExecutionPolicyInfoProvider(),
                    new RetryPolicyInfoProvider()),
                new PoolSizeByHostDistanceInfoProvider(),
                new AuthProviderInfoProvider(),
                new DataCentersInfoProvider(),
                new OtherOptionsInfoProvider(),
                new ConfigAntiPatternsInfoProvider(),
                new ReconnectionPolicyInfoProvider(),
                new DriverInfoProvider(),
                new HostnameInfoProvider());

        internal IContactPointParser ContactPointParser { get; }

        internal IServerNameResolver ServerNameResolver { get; }

        internal Configuration() :
            this(Policies.DefaultPolicies,
                 new ProtocolOptions(),
                 null,
                 new SocketOptions(),
                 new ClientOptions(),
                 NoneAuthProvider.Instance,
                 null,
                 new QueryOptions(),
                 new DefaultAddressTranslator(),
                 new Dictionary<string, IExecutionProfile>(),
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null)
        {
        }

        /// <summary>
        /// Creates a new instance. This class is also used to shareable a context across all instance that are created below one Cluster instance.
        /// One configuration instance per Cluster instance.
        /// </summary>
        internal Configuration(Policies policies,
                               ProtocolOptions protocolOptions,
                               PoolingOptions poolingOptions,
                               SocketOptions socketOptions,
                               ClientOptions clientOptions,
                               IAuthProvider authProvider,
                               IAuthInfoProvider authInfoProvider,
                               QueryOptions queryOptions,
                               IAddressTranslator addressTranslator,
                               IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
                               MetadataSyncOptions metadataSyncOptions,
                               IEndPointResolver endPointResolver,
                               IDriverMetricsProvider driverMetricsProvider,
                               DriverMetricsOptions metricsOptions,
                               string sessionName,
                               GraphOptions graphOptions,
                               Guid? clusterId,
                               string appVersion,
                               string appName,
                               MonitorReportingOptions monitorReportingOptions,
                               TypeSerializerDefinitions typeSerializerDefinitions,
                               bool? keepContactPointsUnresolved,
                               bool? allowBetaProtocolVersions,
                               ISessionFactory sessionFactory = null,
                               IRequestOptionsMapper requestOptionsMapper = null,
                               IStartupOptionsFactory startupOptionsFactory = null,
                               IInsightsSupportVerifier insightsSupportVerifier = null,
                               IRequestHandlerFactory requestHandlerFactory = null,
                               IHostConnectionPoolFactory hostConnectionPoolFactory = null,
                               IRequestExecutionFactory requestExecutionFactory = null,
                               IConnectionFactory connectionFactory = null,
                               IControlConnectionFactory controlConnectionFactory = null,
                               IPrepareHandlerFactory prepareHandlerFactory = null,
                               ITimerFactory timerFactory = null,
                               IObserverFactoryBuilder observerFactoryBuilder = null,
                               IInsightsClientFactory insightsClientFactory = null,
                               IContactPointParser contactPointParser = null,
                               IServerNameResolver serverNameResolver = null,
                               IDnsResolver dnsResolver = null,
                               IMetadataRequestHandler metadataRequestHandler = null,
                               ITopologyRefresherFactory topologyRefresherFactory = null,
                               ISchemaParserFactory schemaParserFactory = null,
                               ISupportedOptionsInitializerFactory supportedOptionsInitializerFactory = null,
                               IProtocolVersionNegotiator protocolVersionNegotiator = null,
                               IServerEventsSubscriber serverEventsSubscriber = null)
        {
            AddressTranslator = addressTranslator ?? throw new ArgumentNullException(nameof(addressTranslator));
            QueryOptions = queryOptions ?? throw new ArgumentNullException(nameof(queryOptions));
            GraphOptions = graphOptions ?? new GraphOptions();
            
            ClusterId = clusterId ?? Guid.NewGuid();
            ApplicationVersion = appVersion ?? Configuration.DefaultApplicationVersion;
            ApplicationName = appName ?? Configuration.FallbackApplicationName;
            ApplicationNameWasGenerated = appName == null;

            Policies = policies;
            ProtocolOptions = protocolOptions;
            PoolingOptions = poolingOptions;
            SocketOptions = socketOptions;
            ClientOptions = clientOptions;
            AuthProvider = authProvider;
            AuthInfoProvider = authInfoProvider;
            StartupOptionsFactory = startupOptionsFactory ?? new StartupOptionsFactory(ClusterId, ApplicationVersion, ApplicationName);
            SessionFactory = sessionFactory ?? new SessionFactory();
            RequestOptionsMapper = requestOptionsMapper ?? new RequestOptionsMapper();
            MetadataSyncOptions = metadataSyncOptions?.Clone() ?? new MetadataSyncOptions();
            DnsResolver = dnsResolver ?? new DnsResolver();
            MetadataRequestHandler = metadataRequestHandler ?? new MetadataRequestHandler();
            TopologyRefresherFactory = topologyRefresherFactory ?? new TopologyRefresherFactory();
            SchemaParserFactory = schemaParserFactory ?? new SchemaParserFactory();
            SupportedOptionsInitializerFactory = supportedOptionsInitializerFactory ?? new SupportedOptionsInitializerFactory();
            ProtocolVersionNegotiator = protocolVersionNegotiator ?? new ProtocolVersionNegotiator();
            ServerEventsSubscriber = serverEventsSubscriber ?? new ServerEventsSubscriber();

            MetricsOptions = metricsOptions ?? new DriverMetricsOptions();
            MetricsProvider = driverMetricsProvider ?? new NullDriverMetricsProvider();
            SessionName = sessionName;
            MetricsEnabled = driverMetricsProvider != null;
            TypeSerializers = typeSerializerDefinitions?.Definitions;
            KeepContactPointsUnresolved = keepContactPointsUnresolved ?? false;
            AllowBetaProtocolVersions = allowBetaProtocolVersions ?? false;
            
            ObserverFactoryBuilder = observerFactoryBuilder ?? (MetricsEnabled ? (IObserverFactoryBuilder)new MetricsObserverFactoryBuilder() : new NullObserverFactoryBuilder());
            RequestHandlerFactory = requestHandlerFactory ?? new RequestHandlerFactory();
            HostConnectionPoolFactory = hostConnectionPoolFactory ?? new HostConnectionPoolFactory();
            RequestExecutionFactory = requestExecutionFactory ?? new RequestExecutionFactory();
            ConnectionFactory = connectionFactory ?? new ConnectionFactory();
            ControlConnectionFactory = controlConnectionFactory ?? new ControlConnectionFactory();
            PrepareHandlerFactory = prepareHandlerFactory ?? new PrepareHandlerFactory();
            TimerFactory = timerFactory ?? new TaskBasedTimerFactory();

            RequestOptions = RequestOptionsMapper.BuildRequestOptionsDictionary(executionProfiles, policies, socketOptions, clientOptions, queryOptions, GraphOptions);
            ExecutionProfiles = BuildExecutionProfilesDictionary(executionProfiles, RequestOptions);
            
            MonitorReportingOptions = monitorReportingOptions ?? new MonitorReportingOptions();
            InsightsSupportVerifier = insightsSupportVerifier ?? Configuration.DefaultInsightsSupportVerifier;
            InsightsClientFactory = insightsClientFactory ?? Configuration.DefaultInsightsClientFactory;
            ServerNameResolver = serverNameResolver ?? new ServerNameResolver(ProtocolOptions);
            EndPointResolver = endPointResolver ?? new EndPointResolver(ServerNameResolver);
            ContactPointParser = contactPointParser ?? new ContactPointParser(DnsResolver, ProtocolOptions, ServerNameResolver, KeepContactPointsUnresolved);

            // Create the buffer pool with 16KB for small buffers and 256Kb for large buffers.
            // The pool does not eagerly reserve the buffers, so it doesn't take unnecessary memory
            // to create the instance.
            BufferPool = new RecyclableMemoryStreamManager(16 * 1024, 256 * 1024, ProtocolOptions.MaximumFrameLength);
            Timer = new HashedWheelTimer();
        }

        /// <summary>
        /// Clones (shallow) the provided execution profile dictionary and add the default profile if not there yet.
        /// </summary>
        private IReadOnlyDictionary<string, IExecutionProfile> BuildExecutionProfilesDictionary(
            IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
            IReadOnlyDictionary<string, IRequestOptions> requestOptions)
        {
            var executionProfilesDictionary = executionProfiles.ToDictionary(profileKvp => profileKvp.Key, profileKvp => profileKvp.Value);
            var defaultOptions = requestOptions[Configuration.DefaultExecutionProfileName];
            executionProfilesDictionary[Configuration.DefaultExecutionProfileName] = new ExecutionProfile(defaultOptions);
            return executionProfilesDictionary;
        }

        /// <summary>
        /// Gets the pooling options. If not specified, creates a new instance with the default by protocol version.
        /// This instance is not stored.
        /// </summary>
        internal PoolingOptions GetOrCreatePoolingOptions(ProtocolVersion protocolVersion)
        {
            return PoolingOptions ?? PoolingOptions.Create(protocolVersion);
        }

        internal int? GetHeartBeatInterval()
        {
            return PoolingOptions != null ? PoolingOptions.GetHeartBeatInterval() : PoolingOptions.Create().GetHeartBeatInterval();
        }

        /// <summary>
        /// Sets the default consistency level.
        /// </summary>
        internal void SetDefaultConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            QueryOptions.SetDefaultConsistencyLevel(consistencyLevel);
        }
    }
}