//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;

using Cassandra.Insights.Schema.StartupMessage;

namespace Cassandra.Insights.InfoProviders.StartupMessage
{
    internal class SpeculativeExecutionPolicyInfoProvider : IPolicyInfoMapper<ISpeculativeExecutionPolicy>
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

        public PolicyInfo GetPolicyInformation(ISpeculativeExecutionPolicy policy)
        {
            return SpeculativeExecutionPolicyInfoProvider.GetSpeculativeExecutionPolicyInfo(policy);
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