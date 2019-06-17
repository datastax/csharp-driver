//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

using Dse.Insights.Schema;

namespace Dse.Insights.MessageFactories
{
    internal class InsightsMetadataFactory : IInsightsMetadataFactory
    {
        private readonly IInsightsMetadataTimestampGenerator _unixTimestampGenerator;

        public InsightsMetadataFactory(IInsightsMetadataTimestampGenerator unixTimestampGenerator)
        {
            _unixTimestampGenerator = unixTimestampGenerator;
        }

        public InsightsMetadata CreateInsightsMetadata(string messageName, string mappingId, InsightType insightType)
        {
            var millisecondsSinceEpoch = _unixTimestampGenerator.GenerateTimestamp();

            return new InsightsMetadata
            {
                Name = messageName,
                InsightMappingId = mappingId,
                InsightType = insightType,
                Tags = new Dictionary<string, string>
                {
                    { "language" , "csharp" }
                },
                Timestamp = millisecondsSinceEpoch
            };
        }
    }
}