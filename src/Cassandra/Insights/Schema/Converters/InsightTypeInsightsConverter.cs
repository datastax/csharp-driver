//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Cassandra.Insights.Schema.Converters
{
    internal class InsightTypeInsightsConverter : InsightsEnumConverter<InsightType, string>
    {
        private static readonly IReadOnlyDictionary<InsightType, string> InsightTypeStringMap =
            new Dictionary<InsightType, string>
            {
                { InsightType.Event, "EVENT" }
            };

        protected override IReadOnlyDictionary<InsightType, string> EnumToJsonValueMap => 
            InsightTypeInsightsConverter.InsightTypeStringMap;
    }
}