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
using System.Net;

using Cassandra.Graph;
using Cassandra.Helpers;
using Cassandra.Insights;
using Cassandra.Insights.InfoProviders;
using Cassandra.Insights.InfoProviders.StartupMessage;
using Cassandra.Insights.InfoProviders.StatusMessage;
using Cassandra.Insights.MessageFactories;
using Cassandra.Insights.Schema.StartupMessage;
using Cassandra.Insights.Schema.StatusMessage;
using Cassandra.SessionManagement;

namespace Cassandra
{
    /// <summary>
    /// Represents the configuration of a <see cref="DseCluster"/>.
    /// </summary>
    public class DseConfiguration
    {
        /// <summary>
        /// Gets the configuration related to DSE Cassandra Daemon.
        /// </summary>
        public Configuration CassandraConfiguration { get; protected set; }
        
        internal ISessionFactoryBuilder<IInternalDseCluster, IInternalDseSession> DseSessionFactoryBuilder { get; }

        internal IDseSessionManagerFactory DseSessionManagerFactory { get; }
        
        internal static IDseSessionManagerFactory DefaultDseSessionManagerFactory =>
            new DseSessionManagerFactory(DseConfiguration.DefaultInsightsClientFactory);

        internal static ISessionFactoryBuilder<IInternalDseCluster, IInternalDseSession> GetDefaultDseSessionFactoryBuilder(
            ISessionFactoryBuilder<IInternalCluster, IInternalSession> sessionFactoryBuilder)
        {
            return new DseSessionFactoryBuilder(sessionFactoryBuilder);
        }
        
        internal static IInsightsClientFactory DefaultInsightsClientFactory =>
            new InsightsClientFactory(
                DseConfiguration.DefaultInsightsStartupMessageFactory, DseConfiguration.DefaultInsightsStatusMessageFactory);

        internal static IInsightsMessageFactory<InsightsStartupData> DefaultInsightsStartupMessageFactory =>
            new InsightsStartupMessageFactory(
                DseConfiguration.DefaultInsightsMetadataFactory,
                DseConfiguration.DefaultInsightsInfoProvidersCollection
            );

        internal static IInsightsMessageFactory<InsightsStatusData> DefaultInsightsStatusMessageFactory =>
            new InsightsStatusMessageFactory(
                DseConfiguration.DefaultInsightsMetadataFactory,
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

        /// <summary>
        /// Creates a new instance of <see cref="DseConfiguration"/>.
        /// </summary>
        public DseConfiguration(Configuration cassandraConfiguration, GraphOptions graphOptions) :
            this(
                cassandraConfiguration,
                graphOptions,
                Guid.NewGuid(),
                null,
                null,
                null,
                null,
                null,
                null,
                null)
        {
            MonitorReportingOptions = new MonitorReportingOptions();
        }

        internal DseConfiguration(
            Configuration cassandraConfiguration,
            GraphOptions graphOptions,
            Guid clusterId,
            string appVersion,
            string appName,
            MonitorReportingOptions monitorReportingOptions,
            IAddressTranslator addressTranslator,
            IInsightsSupportVerifier insightsSupportVerifier,
            IDseSessionManagerFactory sessionManagerFactory,
            ISessionFactoryBuilder<IInternalDseCluster, IInternalDseSession> dseSessionFactoryBuilder)
        {
            CassandraConfiguration = cassandraConfiguration ?? throw new ArgumentNullException(nameof(cassandraConfiguration));

            DseSessionFactoryBuilder = dseSessionFactoryBuilder ?? DseConfiguration.GetDefaultDseSessionFactoryBuilder(cassandraConfiguration.SessionFactoryBuilder);
            DseSessionManagerFactory = sessionManagerFactory ?? DseConfiguration.DefaultDseSessionManagerFactory;
        }
    }
}