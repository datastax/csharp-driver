// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Cassandra.Insights.Schema.StartupMessage
{
    [JsonObject]
    internal class PolicyInfo
    {
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("namespace")]
        public string Namespace { get; set; }

        [JsonProperty("options")]
        public Dictionary<string, object> Options { get; set; }
    }
}