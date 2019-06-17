//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

using Dse.Insights.InfoProviders.StartupMessage;
using Dse.Insights.Schema.StartupMessage;

namespace Dse.Insights.InfoProviders
{
    internal class InsightsInfoProvidersCollection
    {
        public InsightsInfoProvidersCollection(
            IInsightsInfoProvider<InsightsPlatformInfo> platformInfoProvider,
            IInsightsInfoProvider<Dictionary<string, ExecutionProfileInfo>> executionProfileInfoProvider,
            IInsightsInfoProvider<PoolSizeByHostDistance> poolSizeByHostDistanceInfoProvider,
            IInsightsInfoProvider<AuthProviderInfo> authProviderInfoProvider,
            IInsightsInfoProvider<HashSet<string>> dataCentersInfoProvider,
            IInsightsInfoProvider<Dictionary<string, object>> otherOptionsInfoProvider,
            IInsightsInfoProvider<Dictionary<string, string>> configAntiPatternsInfoProvider,
            IInsightsInfoProvider<PolicyInfo> reconnectionPolicyInfoProvider,
            IInsightsInfoProvider<DriverInfo> driverInfoProvider,
            IInsightsInfoProvider<string> hostnameProvider)
        {
            PlatformInfoProvider = platformInfoProvider;
            ExecutionProfileInfoProvider = executionProfileInfoProvider;
            PoolSizeByHostDistanceInfoProvider = poolSizeByHostDistanceInfoProvider;
            AuthProviderInfoProvider = authProviderInfoProvider;
            DataCentersInfoProvider = dataCentersInfoProvider;
            OtherOptionsInfoProvider = otherOptionsInfoProvider;
            ConfigAntiPatternsInfoProvider = configAntiPatternsInfoProvider;
            ReconnectionPolicyInfoProvider = reconnectionPolicyInfoProvider;
            DriverInfoProvider = driverInfoProvider;
            HostnameProvider = hostnameProvider;
        }

        public IInsightsInfoProvider<InsightsPlatformInfo> PlatformInfoProvider { get; }

        public IInsightsInfoProvider<Dictionary<string, ExecutionProfileInfo>> ExecutionProfileInfoProvider { get; }

        public IInsightsInfoProvider<PoolSizeByHostDistance> PoolSizeByHostDistanceInfoProvider { get; }

        public IInsightsInfoProvider<AuthProviderInfo> AuthProviderInfoProvider { get; }

        public IInsightsInfoProvider<HashSet<string>> DataCentersInfoProvider { get; }

        public IInsightsInfoProvider<Dictionary<string, object>> OtherOptionsInfoProvider { get; }

        public IInsightsInfoProvider<Dictionary<string, string>> ConfigAntiPatternsInfoProvider { get; }

        public IInsightsInfoProvider<PolicyInfo> ReconnectionPolicyInfoProvider { get; }

        public IInsightsInfoProvider<DriverInfo> DriverInfoProvider { get; }

        public IInsightsInfoProvider<string> HostnameProvider { get; }
    }
}