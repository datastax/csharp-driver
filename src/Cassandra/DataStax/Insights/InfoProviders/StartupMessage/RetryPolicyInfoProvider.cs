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

namespace Cassandra.DataStax.Insights.InfoProviders.StartupMessage
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