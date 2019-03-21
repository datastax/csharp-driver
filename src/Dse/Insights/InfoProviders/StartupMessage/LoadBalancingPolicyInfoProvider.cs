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
    internal class LoadBalancingPolicyInfoProvider : IInsightsInfoProvider<PolicyInfo>
    {
        static LoadBalancingPolicyInfoProvider()
        {
            LoadBalancingPolicyInfoProvider.LoadBalancingPolicyOptionsProviders 
                = new Dictionary<Type, Func<ILoadBalancingPolicy, IPolicyInfoMapper<IReconnectionPolicy>, Dictionary<string, object>>>
            {
                { 
                    typeof(DCAwareRoundRobinPolicy), 
                    (policy, reconnectionPolicyInfoMapper) =>
                    {
                        var typedPolicy = (DCAwareRoundRobinPolicy) policy;
                        return new Dictionary<string, object>
                        {
#pragma warning disable 618
                            { "localDc", typedPolicy.LocalDc }, { "usedHostsPerRemoteDc", typedPolicy.UsedHostsPerRemoteDc }
#pragma warning restore 618
                        };
                    }
                },
                { 
                    typeof(DseLoadBalancingPolicy), 
                    (policy, reconnectionPolicyInfoMapper) =>
                    {
                        var typedPolicy = (DseLoadBalancingPolicy) policy;
                        return new Dictionary<string, object>
                        {
                            { "childPolicy", LoadBalancingPolicyInfoProvider.GetLoadBalancingPolicyInfo(typedPolicy.ChildPolicy, reconnectionPolicyInfoMapper) }
                        };
                    }
                },
                { 
                    typeof(RetryLoadBalancingPolicy), 
                    (policy, reconnectionPolicyInfoMapper) =>
                    {
                        var typedPolicy = (RetryLoadBalancingPolicy) policy;
                        return new Dictionary<string, object>
                        {
                            { "reconnectionPolicy", reconnectionPolicyInfoMapper.GetReconnectionPolicyInformation(typedPolicy.ReconnectionPolicy) },
                            { "loadBalancingPolicy", LoadBalancingPolicyInfoProvider.GetLoadBalancingPolicyInfo(typedPolicy.LoadBalancingPolicy, reconnectionPolicyInfoMapper) }
                        };
                    }
                },
                { 
                    typeof(TokenAwarePolicy), 
                    (policy, reconnectionPolicyInfoMapper) =>
                    {
                        var typedPolicy = (TokenAwarePolicy) policy;
                        return new Dictionary<string, object>
                        {
                            { "childPolicy", LoadBalancingPolicyInfoProvider.GetLoadBalancingPolicyInfo(typedPolicy.ChildPolicy, reconnectionPolicyInfoMapper) }
                        };
                    }
                }
            };
        }

        private static IReadOnlyDictionary<Type, Func<ILoadBalancingPolicy, IPolicyInfoMapper<IReconnectionPolicy>, Dictionary<string, object>>> LoadBalancingPolicyOptionsProviders { get; }
        
        private readonly IPolicyInfoMapper<IReconnectionPolicy> _reconnectionPolicyInfoMapper;

        public LoadBalancingPolicyInfoProvider(IPolicyInfoMapper<IReconnectionPolicy> reconnectionPolicyInfoMapper)
        {
            _reconnectionPolicyInfoMapper = reconnectionPolicyInfoMapper;
        }

        public PolicyInfo GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var loadBalancingPolicy = cluster.Configuration.CassandraConfiguration.Policies.LoadBalancingPolicy;
            return LoadBalancingPolicyInfoProvider.GetLoadBalancingPolicyInfo(loadBalancingPolicy, _reconnectionPolicyInfoMapper);
        }

        private static PolicyInfo GetLoadBalancingPolicyInfo(ILoadBalancingPolicy policy, IPolicyInfoMapper<IReconnectionPolicy> reconnectionPolicyInfoMapper)
        {
            var loadBalancingPolicyType = policy.GetType();
            LoadBalancingPolicyInfoProvider.LoadBalancingPolicyOptionsProviders.TryGetValue(loadBalancingPolicyType, out var loadBalancingPolicyOptionsProvider);

            return new PolicyInfo
            {
                Namespace = loadBalancingPolicyType.Namespace,
                Type = loadBalancingPolicyType.Name,
                Options = loadBalancingPolicyOptionsProvider?.Invoke(policy, reconnectionPolicyInfoMapper)
            };
        }
    }
}