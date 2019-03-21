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
    internal class RetryPolicyInfoProvider : IInsightsInfoProvider<PolicyInfo>
    {
        static RetryPolicyInfoProvider()
        {
            RetryPolicyInfoProvider.RetryPolicyOptionsProviders = new Dictionary<Type, Func<IRetryPolicy, Dictionary<string, object>>>
            {
                {
                    typeof(IdempotenceAwareRetryPolicy),
                    policy =>
                    {
                        var typedPolicy = (IdempotenceAwareRetryPolicy) policy;
                        return new Dictionary<string, object>
                        {
                            { "childPolicy", RetryPolicyInfoProvider.GetRetryPolicyInfo(typedPolicy.ChildPolicy) }
                        };
                    }
                },
                {
                    typeof(LoggingRetryPolicy),
                    policy =>
                    {
                        var typedPolicy = (LoggingRetryPolicy) policy;
                        return new Dictionary<string, object>
                        {
                            { "childPolicy", RetryPolicyInfoProvider.GetRetryPolicyInfo(typedPolicy.ChildPolicy) }
                        };
                    }
                },
                {
                    typeof(RetryPolicyExtensions.WrappedExtendedRetryPolicy),
                    policy =>
                    {
                        var typedPolicy = (RetryPolicyExtensions.WrappedExtendedRetryPolicy) policy;
                        return new Dictionary<string, object>
                        {
                            { "policy", RetryPolicyInfoProvider.GetRetryPolicyInfo(typedPolicy.Policy) },
                            { "defaultPolicy", RetryPolicyInfoProvider.GetRetryPolicyInfo(typedPolicy.DefaultPolicy) }
                        };
                    }
                }
            };
        }

        public PolicyInfo GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var retryPolicy = cluster.Configuration.CassandraConfiguration.Policies.RetryPolicy;
            return RetryPolicyInfoProvider.GetRetryPolicyInfo(retryPolicy);
        }

        private static IReadOnlyDictionary<Type, Func<IRetryPolicy, Dictionary<string, object>>> RetryPolicyOptionsProviders { get; }
        
        private static PolicyInfo GetRetryPolicyInfo(IRetryPolicy policy)
        {
            var retryPolicyType = policy.GetType();
            RetryPolicyInfoProvider.RetryPolicyOptionsProviders.TryGetValue(retryPolicyType, out var retryPolicyOptionsProvider);

            return new PolicyInfo
            {
                Namespace = retryPolicyType.Namespace,
                Type = retryPolicyType.Name,
                Options = retryPolicyOptionsProvider?.Invoke(policy)
            };
        }
    }
}