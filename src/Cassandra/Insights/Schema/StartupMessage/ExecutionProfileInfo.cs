//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using Cassandra.Insights.Schema.Converters;
using Newtonsoft.Json;

namespace Cassandra.Insights.Schema.StartupMessage
{
    [JsonObject]
    internal class ExecutionProfileInfo
    {
        [JsonProperty("readTimeout")]
        public int? ReadTimeout { get; set; }
        
        [JsonProperty("retry")]
        public PolicyInfo Retry { get; set; }

        [JsonProperty("loadBalancing")]
        public PolicyInfo LoadBalancing { get; set; }

        [JsonProperty("speculativeExecution")]
        public PolicyInfo SpeculativeExecution { get; set; }

        [JsonProperty("consistency")]
        [JsonConverter(typeof(ConsistencyInsightsConverter))]
        public ConsistencyLevel? Consistency { get; set; }

        [JsonProperty("serialConsistency")]
        [JsonConverter(typeof(ConsistencyInsightsConverter))]
        public ConsistencyLevel? SerialConsistency { get; set; }

        [JsonProperty("graphOptions")]
        public Dictionary<string, object> GraphOptions { get; set; }
    }
}