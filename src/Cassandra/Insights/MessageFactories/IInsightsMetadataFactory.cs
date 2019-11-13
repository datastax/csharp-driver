// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Cassandra.Insights.Schema;

namespace Cassandra.Insights.MessageFactories
{
    internal interface IInsightsMetadataFactory
    {
        InsightsMetadata CreateInsightsMetadata(string messageName, string mappingId, InsightType insightType);
    }
}