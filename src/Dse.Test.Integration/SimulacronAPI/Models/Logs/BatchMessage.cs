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
using Dse.Test.Integration.SimulacronAPI.Models.Converters;
using Newtonsoft.Json;

namespace Dse.Test.Integration.SimulacronAPI.Models.Logs
{
    public class BatchMessage : BaseMessage
    {
        [JsonProperty("queries_or_ids")]
        public List<string> QueriesOrIds { get; set; }
        
        [JsonProperty("values")]
        public List<List<string>> Values { get; set; }
        
        [JsonProperty("consistency")]
        [JsonConverter(typeof(ConsistencyLevelEnumConverter))]
        public ConsistencyLevel ConsistencyLevel { get; set; }

        [JsonProperty("serial_consistency")]
        [JsonConverter(typeof(ConsistencyLevelEnumConverter))]
        public ConsistencyLevel? SerialConsistencyLevel { get; set; }

        [JsonProperty("default_timestamp")]
        public long DefaultTimestamp { get; set; }

        [JsonProperty("keyspace")]
        public string Keyspace { get; set; }
    }
}