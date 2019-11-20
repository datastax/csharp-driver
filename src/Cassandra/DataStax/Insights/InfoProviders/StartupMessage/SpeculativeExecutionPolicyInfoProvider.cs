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