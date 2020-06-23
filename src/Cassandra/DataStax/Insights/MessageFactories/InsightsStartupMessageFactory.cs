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

using System.Linq;
using Cassandra.DataStax.Insights.InfoProviders;
using Cassandra.DataStax.Insights.Schema;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights.MessageFactories
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

        public Insight<InsightsStartupData> CreateMessage(IInternalCluster cluster, IInternalSession session, Metadata metadata)
        {
            var insightsMetadata = _metadataFactory.CreateInsightsMetadata(
                InsightsStartupMessageFactory.StartupMessageName, InsightsStartupMessageFactory.StartupV1MappingId, InsightType.Event);

            var driverInfo = _infoProviders.DriverInfoProvider.GetInformation(cluster, session, metadata);
            var startupData = new InsightsStartupData
            {
                ClientId = cluster.Configuration.ClusterId.ToString(),
                SessionId = session.InternalSessionId.ToString(),
                DriverName = driverInfo.DriverName,
                DriverVersion = driverInfo.DriverVersion,
                ApplicationName = cluster.Configuration.ApplicationName,
                ApplicationVersion = cluster.Configuration.ApplicationVersion,
                ApplicationNameWasGenerated = cluster.Configuration.ApplicationNameWasGenerated,
                ContactPoints = 
                    metadata.ResolvedContactPoints.ToDictionary(
                        kvp => kvp.Key.StringRepresentation, kvp => kvp.Value.Select(ipEndPoint => ipEndPoint.GetHostIpEndPointWithFallback().ToString()).ToList()),
                DataCenters = _infoProviders.DataCentersInfoProvider.GetInformation(cluster, session, metadata),
                InitialControlConnection = metadata.ControlConnection.EndPoint?.GetHostIpEndPointWithFallback().ToString(),
                LocalAddress = metadata.ControlConnection.LocalAddress?.ToString(),
                HostName = _infoProviders.HostnameProvider.GetInformation(cluster, session, metadata),
                ProtocolVersion = (byte)metadata.ProtocolVersion,
                ExecutionProfiles = _infoProviders.ExecutionProfileInfoProvider.GetInformation(cluster, session, metadata),
                PoolSizeByHostDistance = _infoProviders.PoolSizeByHostDistanceInfoProvider.GetInformation(cluster, session, metadata),
                HeartbeatInterval = 
                    cluster
                        .Configuration
                        .GetOrCreatePoolingOptions(metadata.ProtocolVersion)
                        .GetHeartBeatInterval() ?? 0,
                Compression = cluster.Configuration.ProtocolOptions.Compression,
                ReconnectionPolicy = _infoProviders.ReconnectionPolicyInfoProvider.GetInformation(cluster, session, metadata),
                Ssl = new SslInfo { Enabled = cluster.Configuration.ProtocolOptions.SslOptions != null },
                AuthProvider = _infoProviders.AuthProviderInfoProvider.GetInformation(cluster, session, metadata),
                OtherOptions = _infoProviders.OtherOptionsInfoProvider.GetInformation(cluster, session, metadata),
                PlatformInfo = _infoProviders.PlatformInfoProvider.GetInformation(cluster, session, metadata),
                ConfigAntiPatterns = _infoProviders.ConfigAntiPatternsInfoProvider.GetInformation(cluster, session, metadata),
                PeriodicStatusInterval = cluster.Configuration.MonitorReportingOptions.StatusEventDelayMilliseconds / 1000
            };

            return new Insight<InsightsStartupData>
            {
                Metadata = insightsMetadata,
                Data = startupData
            };
        }
    }
}