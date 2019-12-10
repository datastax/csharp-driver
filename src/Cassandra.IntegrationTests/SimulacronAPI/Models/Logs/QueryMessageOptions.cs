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
using Cassandra.IntegrationTests.SimulacronAPI.Models.Converters;
using Newtonsoft.Json;

namespace Cassandra.IntegrationTests.SimulacronAPI.Models.Logs
{
    public class QueryMessageOptions
    {
        [JsonProperty("consistency")]
        [JsonConverter(typeof(ConsistencyLevelEnumConverter))]
        public ConsistencyLevel ConsistencyLevel { get; set; }
        
        [JsonProperty("serial_consistency")]
        [JsonConverter(typeof(ConsistencyLevelEnumConverter))]
        public ConsistencyLevel? SerialConsistencyLevel { get; set; }
        
        [JsonProperty("positional_values")]
        public List<object> PositionalValues { get; set; }
        
        [JsonProperty("named_values")]
        public Dictionary<string, object> NamedValues { get; set; }
        
        [JsonProperty("skip_metadata")]
        public bool? SkipMetadata { get; set; }
        
        [JsonProperty("page_size")]
        public int? PageSize { get; set; }
        
        [JsonProperty("paging_state")]
        public string PagingState { get; set; }
        
        [JsonProperty("default_timestamp")]
        public long DefaultTimestamp { get; set; }
        
        [JsonProperty("keyspace")]
        public string Keyspace { get; set; }
    }
}