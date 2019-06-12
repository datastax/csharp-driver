﻿//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Net;

using Dse.Graph;
using Dse.Helpers;
using Dse.Insights;
using Dse.Insights.InfoProviders;
using Dse.Insights.InfoProviders.StartupMessage;
using Dse.Insights.InfoProviders.StatusMessage;
using Dse.Insights.MessageFactories;
using Dse.Insights.Schema.StartupMessage;
using Dse.Insights.Schema.StatusMessage;
using Dse.SessionManagement;

namespace Dse
{
    /// <summary>
    /// Represents the configuration of a <see cref="DseCluster"/>.
    /// </summary>
    public class DseConfiguration
    {
        internal static string DefaultApplicationVersion => string.Empty;

        internal static string FallbackApplicationName =>
            AssemblyHelpers.GetEntryAssembly()?.GetName().Name ?? DseClusterBuilder.DefaultApplicationName;

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
        /// To be replaced with CassandraConfiguration.AddressTranslator after CSHARP-444.
        /// </summary>
        internal IAddressTranslator AddressTranslator { get; }

        /// <summary>
        /// Gets the configuration related to DSE Cassandra Daemon.
        /// </summary>
        public Configuration CassandraConfiguration { get; protected set; }

        /// <summary>
        /// Gets the options related to graph instance.
        /// </summary>
        public GraphOptions GraphOptions { get; protected set; }

        internal ISessionFactoryBuilder<IInternalDseCluster, IInternalDseSession> DseSessionFactoryBuilder { get; }

        internal IDseSessionManagerFactory DseSessionManagerFactory { get; }

        /// <summary>
        /// Configuration options for monitor reporting
        /// </summary>
        public MonitorReportingOptions MonitorReportingOptions { get; }

        internal IInsightsSupportVerifier InsightsSupportVerifier { get; }

        internal static IDseSessionManagerFactory DefaultDseSessionManagerFactory =>
            new DseSessionManagerFactory(DseConfiguration.DefaultInsightsClientFactory);

        internal static ISessionFactoryBuilder<IInternalDseCluster, IInternalDseSession> GetDefaultDseSessionFactoryBuilder(
            ISessionFactoryBuilder<IInternalCluster, IInternalSession> sessionFactoryBuilder)
        {
            return new DseSessionFactoryBuilder(sessionFactoryBuilder);
        }

        internal static IInsightsSupportVerifier DefaultInsightsSupportVerifier => new InsightsSupportVerifier();

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
            ApplicationName = DseConfiguration.FallbackApplicationName;
            ApplicationNameWasGenerated = true;
            ClusterId = Guid.NewGuid();
            ApplicationVersion = DseConfiguration.DefaultApplicationVersion;
            MonitorReportingOptions = new MonitorReportingOptions();

            AddressTranslator = new IdentityAddressTranslator();
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
            GraphOptions = graphOptions ?? throw new ArgumentNullException(nameof(graphOptions));

            ClusterId = clusterId;
            ApplicationVersion = appVersion ?? DseConfiguration.DefaultApplicationVersion;
            ApplicationName = appName ?? DseConfiguration.FallbackApplicationName;
            ApplicationNameWasGenerated = appName == null;

            AddressTranslator = addressTranslator ?? new IdentityAddressTranslator();

            MonitorReportingOptions = monitorReportingOptions ?? new MonitorReportingOptions();
            InsightsSupportVerifier = insightsSupportVerifier ?? DseConfiguration.DefaultInsightsSupportVerifier;
            DseSessionFactoryBuilder = dseSessionFactoryBuilder ?? DseConfiguration.GetDefaultDseSessionFactoryBuilder(cassandraConfiguration.SessionFactoryBuilder);
            DseSessionManagerFactory = sessionManagerFactory ?? DseConfiguration.DefaultDseSessionManagerFactory;
        }
    }

    internal class IdentityAddressTranslator : IAddressTranslator
    {
        public IPEndPoint Translate(IPEndPoint address)
        {
            return address;
        }
    }
}