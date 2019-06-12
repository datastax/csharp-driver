﻿//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using Dse.Insights.Schema.Converters;
using Newtonsoft.Json;

namespace Dse.Insights.Schema
{
    [JsonObject]
    internal class InsightsMetadata
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("timestamp")]
        public long Timestamp { get; set; }

        [JsonProperty("tags")]
        public Dictionary<string, string> Tags { get; set; }

        [JsonProperty("insightType")]
        [JsonConverter(typeof(InsightTypeInsightsConverter))]
        public InsightType InsightType { get; set; }

        [JsonProperty("insightMappingId")]
        public string InsightMappingId { get; set; }
    }
}