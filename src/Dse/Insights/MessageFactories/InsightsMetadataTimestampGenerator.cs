// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;

namespace Dse.Insights.MessageFactories
{
    internal class InsightsMetadataTimestampGenerator : IInsightsMetadataTimestampGenerator
    {
        private static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public long GenerateTimestamp()
        {
            var t = DateTimeOffset.UtcNow - InsightsMetadataTimestampGenerator.UnixEpoch;
            return (long) t.TotalMilliseconds;
        }
    }
}