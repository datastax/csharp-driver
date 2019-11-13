// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

namespace Cassandra.Insights.MessageFactories
{
    internal interface IInsightsMetadataTimestampGenerator
    {
        long GenerateTimestamp();
    }
}