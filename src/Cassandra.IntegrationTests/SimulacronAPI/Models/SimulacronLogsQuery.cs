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

using Cassandra.IntegrationTests.SimulacronAPI.Models.Converters;
using Newtonsoft.Json;

namespace Cassandra.IntegrationTests.SimulacronAPI.Models
{
    public class SimulacronLogsQuery
    {
        [JsonProperty("query")]
        public string Query { get; set; }
        
        [JsonProperty("consistency_level")]
        [JsonConverter(typeof(ConsistencyLevelEnumConverter))]
        public ConsistencyLevel? ConsistencyLevel { get; set; }
        
        [JsonProperty("serial_consistency_level")]
        [JsonConverter(typeof(ConsistencyLevelEnumConverter))]
        public ConsistencyLevel? SerialConsistencyLevel { get; set; }

        [JsonProperty("connection")]
        public string Connection { get; set; }

        [JsonProperty("received_timestamp")]
        public long ReceivedTimestamp { get; set; }
        
        [JsonProperty("client_timestamp")]
        public long ClientTimestamp { get; set; }
        
        [JsonProperty("primed")]
        public bool Primed { get; set; }
        
        [JsonProperty("type")]
        public string Type { get; set; }
        
        [JsonProperty("frame")]
        public SimulacronLogsQueryFrame Frame { get; set; }
    }
}