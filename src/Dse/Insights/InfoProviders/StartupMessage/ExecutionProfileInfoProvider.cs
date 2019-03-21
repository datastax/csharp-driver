//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using Dse.Insights.Schema.Converters;
using Dse.Insights.Schema.StartupMessage;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class ExecutionProfileInfoProvider : IInsightsInfoProvider<Dictionary<string, ExecutionProfileInfo>>
    {
        private readonly IInsightsInfoProvider<PolicyInfo> _loadBalancingPolicyInfoProvider;
        private readonly IInsightsInfoProvider<PolicyInfo> _speculativeExecutionPolicyInfoProvider;
        private readonly IInsightsInfoProvider<PolicyInfo> _retryPolicyInfoProvider;

        public ExecutionProfileInfoProvider(
            IInsightsInfoProvider<PolicyInfo> loadBalancingPolicyInfoProvider,
            IInsightsInfoProvider<PolicyInfo> speculativeExecutionPolicyInfoProvider,
            IInsightsInfoProvider<PolicyInfo> retryPolicyInfoProvider)
        {
            _loadBalancingPolicyInfoProvider = loadBalancingPolicyInfoProvider;
            _speculativeExecutionPolicyInfoProvider = speculativeExecutionPolicyInfoProvider;
            _retryPolicyInfoProvider = retryPolicyInfoProvider;
        }

        public Dictionary<string, ExecutionProfileInfo> GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var consistencyConverter = new ConsistencyInsightsConverter();

            string graphReadConsistency = null;
            if (cluster.Configuration.GraphOptions.ReadConsistencyLevel.HasValue)
            {
                consistencyConverter.TryConvert(cluster.Configuration.GraphOptions.ReadConsistencyLevel.Value, out graphReadConsistency);
            }

            string graphWriteConsistency = null;
            if (cluster.Configuration.GraphOptions.WriteConsistencyLevel.HasValue)
            {
                consistencyConverter.TryConvert(cluster.Configuration.GraphOptions.WriteConsistencyLevel.Value, out graphWriteConsistency);
            }

            return new Dictionary<string, ExecutionProfileInfo>
            {
                {
                    "default",
                    new ExecutionProfileInfo
                    {
                        Consistency = cluster.Configuration.CassandraConfiguration.QueryOptions.GetConsistencyLevel(),
                        SerialConsistency = cluster.Configuration.CassandraConfiguration.QueryOptions.GetSerialConsistencyLevel(),
                        GraphOptions = new Dictionary<string, object>
                        {
                            { "language", cluster.Configuration.GraphOptions.Language },
                            { "source", cluster.Configuration.GraphOptions.Source },
                            { "name", cluster.Configuration.GraphOptions.Name },
                            { "readConsistency", graphReadConsistency },
                            { "writeConsistency", graphWriteConsistency },
                            { "readTimeout", cluster.Configuration.GraphOptions.ReadTimeoutMillis }
                        },
                        LoadBalancing = _loadBalancingPolicyInfoProvider.GetInformation(cluster, dseSession),
                        ReadTimeout = cluster.Configuration.CassandraConfiguration.SocketOptions.ReadTimeoutMillis,
                        SpeculativeExecution = _speculativeExecutionPolicyInfoProvider.GetInformation(cluster, dseSession),
                        Retry = _retryPolicyInfoProvider.GetInformation(cluster, dseSession)
                    }
                }
            };
        }
    }
}