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
    internal class SpeculativeExecutionPolicyInfoProvider : IInsightsInfoProvider<PolicyInfo>
    {
        static SpeculativeExecutionPolicyInfoProvider()
        {
            SpeculativeExecutionPolicyInfoProvider.SpeculativeExecutionPolicyOptionsProviders 
                = new Dictionary<Type, Func<ISpeculativeExecutionPolicy, Dictionary<string, object>>>
                {
                    { 
                        typeof(ConstantSpeculativeExecutionPolicy), 
                        policy =>
                        {
                            var typedPolicy = (ConstantSpeculativeExecutionPolicy) policy;
                            return new Dictionary<string, object>
                            {
                                { "delay", typedPolicy.Delay }, { "maxSpeculativeExecutions", typedPolicy.MaxSpeculativeExecutions }
                            };
                        }
                    }
                };
        }
        
        private static IReadOnlyDictionary<Type, Func<ISpeculativeExecutionPolicy, Dictionary<string, object>>> SpeculativeExecutionPolicyOptionsProviders { get; }

        public PolicyInfo GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var speculativeExecutionPolicy = cluster.Configuration.CassandraConfiguration.Policies.SpeculativeExecutionPolicy;
            return SpeculativeExecutionPolicyInfoProvider.GetSpeculativeExecutionPolicyInfo(speculativeExecutionPolicy);
        }

        private static PolicyInfo GetSpeculativeExecutionPolicyInfo(ISpeculativeExecutionPolicy policy)
        {
            var speculativeExecutionPolicyType = policy.GetType();
            SpeculativeExecutionPolicyInfoProvider.SpeculativeExecutionPolicyOptionsProviders.TryGetValue(speculativeExecutionPolicyType, out var speculativeExecutionPolicyOptionsProvider);

            return new PolicyInfo
            {
                Namespace = speculativeExecutionPolicyType.Namespace,
                Type = speculativeExecutionPolicyType.Name,
                Options = speculativeExecutionPolicyOptionsProvider?.Invoke(policy)
            };
        }
    }
}