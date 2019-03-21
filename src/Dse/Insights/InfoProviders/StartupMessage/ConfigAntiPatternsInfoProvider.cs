// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class ConfigAntiPatternsInfoProvider : IInsightsInfoProvider<Dictionary<string, string>>
    {
        static ConfigAntiPatternsInfoProvider()
        {
            ConfigAntiPatternsInfoProvider.AntiPatternsProviders = new Dictionary<Type, Func<object, Dictionary<string, string>, Dictionary<string, string>>>
            {
                { 
                    typeof(DCAwareRoundRobinPolicy), 
                    (obj, antiPatterns) =>
                    {
                        var typedPolicy = (DCAwareRoundRobinPolicy) obj;
#pragma warning disable 618
                        if (typedPolicy.UsedHostsPerRemoteDc > 0)
#pragma warning restore 618
                        {
                            antiPatterns["useRemoteHosts"] = "Using remote hosts for fail-over";
                        }

                        return antiPatterns;
                    }
                },
                { 
#pragma warning disable 618
                    typeof(DowngradingConsistencyRetryPolicy),
#pragma warning restore 618
                    (obj, antiPatterns) =>
                    {
                        antiPatterns["downgradingConsistency"] = "Downgrading consistency retry policy in use";
                        return antiPatterns;
                    }
                },
                { 
                    typeof(DseLoadBalancingPolicy), 
                    (obj, antiPatterns) =>
                    {
                        var typedPolicy = (DseLoadBalancingPolicy) obj;
                        return ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.ChildPolicy, antiPatterns);
                    }
                },
                { 
                    typeof(RetryLoadBalancingPolicy), 
                    (obj, antiPatterns) =>
                    {
                        var typedPolicy = (RetryLoadBalancingPolicy) obj;
                        antiPatterns =  ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.ReconnectionPolicy, antiPatterns);
                        return ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.LoadBalancingPolicy, antiPatterns);
                    }
                },
                { 
                    typeof(TokenAwarePolicy), 
                    (obj, antiPatterns) =>
                    {
                        var typedPolicy = (TokenAwarePolicy) obj;
                        return ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.ChildPolicy, antiPatterns);
                    }
                },
                {
                    typeof(IdempotenceAwareRetryPolicy),
                    (obj, antiPatterns) =>
                    {
                        var typedPolicy = (IdempotenceAwareRetryPolicy) obj;
                        return ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.ChildPolicy, antiPatterns);
                    }
                },
                {
                    typeof(LoggingRetryPolicy),
                    (obj, antiPatterns) =>
                    {
                        var typedPolicy = (LoggingRetryPolicy) obj;
                        return ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.ChildPolicy, antiPatterns);
                    }
                },
                {
                    typeof(RetryPolicyExtensions.WrappedExtendedRetryPolicy),
                    (obj, antiPatterns) =>
                    {
                        var typedPolicy = (RetryPolicyExtensions.WrappedExtendedRetryPolicy) obj;
                        antiPatterns =  ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.Policy, antiPatterns);
                        return ConfigAntiPatternsInfoProvider.AddAntiPatterns(typedPolicy.DefaultPolicy, antiPatterns);
                    }
                }
            };
        }

        public static IReadOnlyDictionary<Type, Func<object, Dictionary<string, string>, Dictionary<string, string>>> AntiPatternsProviders { get; }

        public Dictionary<string, string> GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var antiPatterns = new Dictionary<string, string>();

            var contactPoints = cluster.Metadata.ResolvedContactPoints.Values.SelectMany(endPoints => endPoints).ToList();
            var contactPointsHosts = cluster.AllHosts().Where(host => contactPoints.Contains(host.Address)).ToList();

            if (contactPointsHosts.Select(c => c.Datacenter).Where(dc => dc != null).Distinct().Count() > 1)
            {
                antiPatterns["contactPointsMultipleDCs"] = "Contact points contain hosts from multiple data centers";
            }

            var loadBalancingPolicy = cluster.Configuration.CassandraConfiguration.Policies.LoadBalancingPolicy;
            antiPatterns = ConfigAntiPatternsInfoProvider.AddAntiPatterns(loadBalancingPolicy, antiPatterns);

            var retryPolicy = cluster.Configuration.CassandraConfiguration.Policies.RetryPolicy;
            antiPatterns = ConfigAntiPatternsInfoProvider.AddAntiPatterns(retryPolicy, antiPatterns);

            return antiPatterns;
        }

        private static Dictionary<string, string> AddAntiPatterns(object obj, Dictionary<string, string> antiPatterns)
        {
            return ConfigAntiPatternsInfoProvider.AntiPatternsProviders.TryGetValue(obj.GetType(), out var provider) 
                ? provider.Invoke(obj, antiPatterns) 
                : antiPatterns;
        }
    }
}