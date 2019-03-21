// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Collections.Generic;
using Dse.Insights.Schema.StartupMessage;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
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

        public PolicyInfo GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var policy = cluster.Configuration.CassandraConfiguration.Policies.ReconnectionPolicy;
            var type = policy.GetType();
            ReconnectionPolicyInfoProvider.PolicyOptionsProviders.TryGetValue(type, out var optionsProvider);
            return new PolicyInfo
            {
                Namespace = type.Namespace,
                Type = type.Name,
                Options = optionsProvider?.Invoke(policy)
            };
        }

        public PolicyInfo GetReconnectionPolicyInformation(IReconnectionPolicy policy)
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