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
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights.InfoProviders.StartupMessage
{
    internal class ReconnectionPolicyInfoProvider : IPolicyInfoMapper<IReconnectionPolicy>, IInsightsInfoProvider<PolicyInfo>
    {
        static ReconnectionPolicyInfoProvider()
        {
            ReconnectionPolicyInfoProvider.PolicyOptionsProviders = new Dictionary<Type, Func<IReconnectionPolicy, Dictionary<string, object>>>
            {
                { 
                    typeof(ConstantReconnectionPolicy), 
                    policy =>
                    {
                        var typedPolicy = (ConstantReconnectionPolicy) policy;
                        return new Dictionary<string, object> {{ "constantDelayMs", typedPolicy.ConstantDelayMs }};
                    }
                },
                { 
                    typeof(ExponentialReconnectionPolicy), 
                    policy =>
                    {
                        var typedPolicy = (ExponentialReconnectionPolicy) policy;
                        return new Dictionary<string, object> {{ "baseDelayMs", typedPolicy.BaseDelayMs }, { "maxDelayMs", typedPolicy.MaxDelayMs }};
                    }
                },
                { 
                    typeof(FixedReconnectionPolicy), 
                    policy =>
                    {
                        var typedPolicy = (FixedReconnectionPolicy) policy;
                        return new Dictionary<string, object> {{ "delays", typedPolicy.Delays }};
                    }
                }
            };
        }

        public static IReadOnlyDictionary<Type, Func<IReconnectionPolicy, Dictionary<string, object>>> PolicyOptionsProviders { get; }

        public PolicyInfo GetInformation(IInternalCluster cluster, IInternalSession session)
        {
            var policy = cluster.Configuration.Policies.ReconnectionPolicy;
            var type = policy.GetType();
            ReconnectionPolicyInfoProvider.PolicyOptionsProviders.TryGetValue(type, out var optionsProvider);
            return new PolicyInfo
            {
                Namespace = type.Namespace,
                Type = type.Name,
                Options = optionsProvider?.Invoke(policy)
            };
        }

        public PolicyInfo GetPolicyInformation(IReconnectionPolicy policy)
        {
            var type = policy.GetType();
            ReconnectionPolicyInfoProvider.PolicyOptionsProviders.TryGetValue(type, out var optionsProvider);
            return new PolicyInfo
            {
                Namespace = type.Namespace,
                Type = type.Name,
                Options = optionsProvider?.Invoke(policy)
            };
        }
    }
}