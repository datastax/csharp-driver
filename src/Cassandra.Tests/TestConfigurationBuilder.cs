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
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Insights;
using Cassandra.ExecutionProfiles;
using Cassandra.MetadataHelpers;
using Cassandra.Metrics;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Observers;
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

namespace Cassandra.Tests
{
    internal class TestConfigurationBuilder
    {
        public Cassandra.Policies Policies { get; set; } = Cassandra.Policies.DefaultPolicies;

        public ProtocolOptions ProtocolOptions { get; set; } = new ProtocolOptions();

        public PoolingOptions PoolingOptions { get; set; } = new PoolingOptions();

        public SocketOptions SocketOptions { get; set; } = new SocketOptions();

        public ClientOptions ClientOptions { get; set; } = new ClientOptions();

        public IAuthProvider AuthProvider { get; set; } = new NoneAuthProvider();

        public IAuthInfoProvider AuthInfoProvider { get; set; } = new SimpleAuthInfoProvider();

        public QueryOptions QueryOptions { get; set; } = new QueryOptions();

        public IAddressTranslator AddressTranslator { get; set; } = new DefaultAddressTranslator();

        public MetadataSyncOptions MetadataSyncOptions { get; set; } = new MetadataSyncOptions();

        public IStartupOptionsFactory StartupOptionsFactory { get; set; } = new StartupOptionsFactory(Guid.NewGuid(), Configuration.DefaultApplicationVersion, Builder.DefaultApplicationName);

        public IRequestOptionsMapper RequestOptionsMapper { get; set; } = new RequestOptionsMapper();

        public ISessionFactory SessionFactory { get; set; } = new SessionFactory();

        public IReadOnlyDictionary<string, IExecutionProfile> ExecutionProfiles { get; set; } = new Dictionary<string, IExecutionProfile>();

        public IRequestHandlerFactory RequestHandlerFactory { get; set; } = new RequestHandlerFactory();

        public IHostConnectionPoolFactory HostConnectionPoolFactory { get; set; } = new HostConnectionPoolFactory();

        public IRequestExecutionFactory RequestExecutionFactory { get; set; } = new RequestExecutionFactory();

        public IConnectionFactory ConnectionFactory { get; set; } = new ConnectionFactory();

        public IControlConnectionFactory ControlConnectionFactory { get; set; } = new ControlConnectionFactory();

        public IPrepareHandlerFactory PrepareHandlerFactory { get; set; } = new PrepareHandlerFactory();

        public ITimerFactory TimerFactory { get; set; } = new TaskBasedTimerFactory();

        public IEndPointResolver EndPointResolver { get; set; }

        public IObserverFactoryBuilder ObserverFactoryBuilder { get; set; } = new MetricsObserverFactoryBuilder(true);

        public DriverMetricsOptions MetricsOptions { get; set; } = new DriverMetricsOptions();

        public string SessionName { get; set; } = Configuration.DefaultSessionName;

        public string ApplicationVersion { get; set; } = Configuration.DefaultApplicationVersion;

        public string ApplicationName { get; set; } = Builder.DefaultApplicationName;

        public Guid ClusterId { get; set; } = Guid.NewGuid();

        public GraphOptions GraphOptions { get; set; } = new GraphOptions();

        public bool? KeepContactPointsUnresolved { get; set; }

        public bool? AllowBetaProtocolVersions { get; set; }

        public IContactPointParser ContactPointParser { get; set; }

        public IServerNameResolver ServerNameResolver { get; set; }

        public IDnsResolver DnsResolver { get; set; }

        public IMetadataRequestHandler MetadataRequestHandler { get; set; } = new MetadataRequestHandler();

        public ITopologyRefresherFactory TopologyRefresherFactory { get; set; } = new TopologyRefresherFactory();

        public ISchemaParserFactory SchemaParserFactory { get; set; } = new SchemaParserFactory();

        public ISupportedOptionsInitializerFactory SupportedOptionsInitializerFactory { get; set; } = new SupportedOptionsInitializerFactory();

        public IServerEventsSubscriber ServerEventsSubscriber { get; set; } = new ServerEventsSubscriber();
        
        public IProtocolVersionNegotiator ProtocolVersionNegotiator { get; set; } = new ProtocolVersionNegotiator();
        
        public MonitorReportingOptions MonitorReportingOptions { get; set; } = new MonitorReportingOptions();

        public IInsightsSupportVerifier InsightsSupportVerifier { get; set; } = new InsightsSupportVerifier();

        public IInsightsClientFactory InsightsClientFactory { get; set; } = 
            new InsightsClientFactory(
                Configuration.DefaultInsightsStartupMessageFactory, Configuration.DefaultInsightsStatusMessageFactory);

        public TypeSerializerDefinitions TypeSerializerDefinitions { get; set; } = new TypeSerializerDefinitions();

        public Configuration Build()
        {
            return new Configuration(
                Policies,
                ProtocolOptions,
                PoolingOptions,
                SocketOptions,
                ClientOptions,
                AuthProvider,
                AuthInfoProvider,
                QueryOptions,
                AddressTranslator,
                ExecutionProfiles,
                MetadataSyncOptions,
                EndPointResolver,
                new NullDriverMetricsProvider(),
                MetricsOptions,
                SessionName,
                GraphOptions,
                ClusterId,
                ApplicationVersion,
                ApplicationName,
                MonitorReportingOptions,
                TypeSerializerDefinitions,
                KeepContactPointsUnresolved,
                AllowBetaProtocolVersions,
                SessionFactory,
                RequestOptionsMapper,
                StartupOptionsFactory,
                InsightsSupportVerifier,
                RequestHandlerFactory,
                HostConnectionPoolFactory,
                RequestExecutionFactory,
                ConnectionFactory,
                ControlConnectionFactory,
                PrepareHandlerFactory,
                TimerFactory,
                ObserverFactoryBuilder,
                InsightsClientFactory,
                ContactPointParser,
                ServerNameResolver,
                DnsResolver,
                MetadataRequestHandler,
                TopologyRefresherFactory,
                SchemaParserFactory,
                SupportedOptionsInitializerFactory,
                ProtocolVersionNegotiator,
                ServerEventsSubscriber);
        }
    }
}