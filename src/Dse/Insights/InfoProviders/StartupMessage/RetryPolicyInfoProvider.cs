//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;

using Dse.Insights.Schema.StartupMessage;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class RetryPolicyInfoProvider : IPolicyInfoMapper<IExtendedRetryPolicy>
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
                }
            };
        }

        public PolicyInfo GetPolicyInformation(IExtendedRetryPolicy policy)
        {
            return RetryPolicyInfoProvider.GetRetryPolicyInfo(policy);
        }

        private static IReadOnlyDictionary<Type, Func<IRetryPolicy, Dictionary<string, object>>> RetryPolicyOptionsProviders { get; }

        private static PolicyInfo GetRetryPolicyInfo(IRetryPolicy policy)
        {
            var retryPolicyType = policy.GetType();

            if (retryPolicyType == typeof(RetryPolicyExtensions.WrappedExtendedRetryPolicy))
            {
                var typedPolicy = (RetryPolicyExtensions.WrappedExtendedRetryPolicy) policy;
                return RetryPolicyInfoProvider.GetRetryPolicyInfo(typedPolicy.Policy);
            }

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