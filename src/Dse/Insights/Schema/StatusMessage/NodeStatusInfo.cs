// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using Newtonsoft.Json;

namespace Dse.Insights.Schema.StatusMessage
{
    [JsonObject]
    internal class NodeStatusInfo
    {
        [JsonProperty("connections")]
        public int Connections { get; set; }

        [JsonProperty("inFlightQueries")]
        public int InFlightQueries { get; set; }
    }
}