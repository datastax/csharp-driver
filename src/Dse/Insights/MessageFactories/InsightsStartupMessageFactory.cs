//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Linq;
using Dse.Insights.InfoProviders;
using Dse.Insights.Schema;
using Dse.Insights.Schema.StartupMessage;
using Dse.SessionManagement;

namespace Dse.Insights.MessageFactories
{
    internal class InsightsStartupMessageFactory : IInsightsMessageFactory<InsightsStartupData>
    {
        private const string StartupMessageName = "driver.startup";
        private const string StartupV1MappingId = "v1";
        
        private readonly IInsightsMetadataFactory _metadataFactory;
        private readonly InsightsInfoProvidersCollection _infoProviders;

        public InsightsStartupMessageFactory(IInsightsMetadataFactory metadataFactory, InsightsInfoProvidersCollection infoProviders)
        {
            _metadataFactory = metadataFactory;
            _infoProviders = infoProviders;
        }

        public Insight<InsightsStartupData> CreateMessage(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var metadata = _metadataFactory.CreateInsightsMetadata(
                InsightsStartupMessageFactory.StartupMessageName, InsightsStartupMessageFactory.StartupV1MappingId, InsightType.Event);

            var driverInfo = _infoProviders.DriverInfoProvider.GetInformation(cluster, dseSession);
            var startupData = new InsightsStartupData
            {
                ClientId = cluster.Configuration.ClusterId.ToString(),
                SessionId = dseSession.InternalSessionId.ToString(),
                DriverName = driverInfo.DriverName,
                DriverVersion = driverInfo.DriverVersion,
                ApplicationName = cluster.Configuration.ApplicationName,
                ApplicationVersion = cluster.Configuration.ApplicationVersion,
                ApplicationNameWasGenerated = cluster.Configuration.ApplicationNameWasGenerated,
                ContactPoints = 
                    cluster.Metadata.ResolvedContactPoints.ToDictionary(
                        kvp => kvp.Key, kvp => kvp.Value.Select(ipEndPoint => ipEndPoint.ToString()).ToList()),
                DataCenters = _infoProviders.DataCentersInfoProvider.GetInformation(cluster, dseSession),
                InitialControlConnection = cluster.Metadata.ControlConnection.Address?.ToString(),
                LocalAddress = cluster.Metadata.ControlConnection.LocalAddress?.ToString(),
                HostName = _infoProviders.HostnameProvider.GetInformation(cluster, dseSession),
                ProtocolVersion = (byte)cluster.Metadata.ControlConnection.ProtocolVersion,
                ExecutionProfiles = _infoProviders.ExecutionProfileInfoProvider.GetInformation(cluster, dseSession),
                PoolSizeByHostDistance = _infoProviders.PoolSizeByHostDistanceInfoProvider.GetInformation(cluster, dseSession),
                HeartbeatInterval = 
                    cluster
                        .Configuration
                        .CassandraConfiguration
                        .GetPoolingOptions(cluster.Metadata.ControlConnection.ProtocolVersion)
                        .GetHeartBeatInterval() ?? 0,
                Compression = cluster.Configuration.CassandraConfiguration.ProtocolOptions.Compression,
                ReconnectionPolicy = _infoProviders.ReconnectionPolicyInfoProvider.GetInformation(cluster, dseSession),
                Ssl = new SslInfo { Enabled = cluster.Configuration.CassandraConfiguration.ProtocolOptions.SslOptions != null },
                AuthProvider = _infoProviders.AuthProviderInfoProvider.GetInformation(cluster, dseSession),
                OtherOptions = _infoProviders.OtherOptionsInfoProvider.GetInformation(cluster, dseSession),
                PlatformInfo = _infoProviders.PlatformInfoProvider.GetInformation(cluster, dseSession),
                ConfigAntiPatterns = _infoProviders.ConfigAntiPatternsInfoProvider.GetInformation(cluster, dseSession),
                PeriodicStatusInterval = cluster.Configuration.MonitorReportingOptions.StatusEventDelayMilliseconds / 1000
            };

            return new Insight<InsightsStartupData>
            {
                Metadata = metadata,
                Data = startupData
            };
        }
    }
}