// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Newtonsoft.Json;

namespace Dse.Insights.Schema
{
    [JsonObject]
    internal class Insight<T>
    {
        [JsonProperty("metadata")]
        public InsightsMetadata Metadata;
        
        [JsonProperty("data")]
        public T Data;
    }
}