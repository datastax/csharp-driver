//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.Collections.Generic;
using Cassandra.DataStax.Insights.Schema.Converters;
using Newtonsoft.Json;

namespace Cassandra.DataStax.Insights.Schema.StartupMessage
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