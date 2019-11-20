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

using System.Collections.Generic;
using Cassandra.DataStax.Insights.InfoProviders.StartupMessage;
using Cassandra.DataStax.Insights.Schema.StartupMessage;

namespace Cassandra.DataStax.Insights.InfoProviders
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