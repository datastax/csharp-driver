// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Cassandra.Insights.Schema.StatusMessage
{
    [JsonObject]
    internal class InsightsStatusData
    {
        [JsonProperty("clientId")]
        public string ClientId { get; set; }

        [JsonProperty("sessionId")]
        public string SessionId { get; set; }
        
        [JsonProperty("controlConnection")]
        public string ControlConnection { get; set; }

        [JsonProperty("connectedNodes")]
        public Dictionary<string, NodeStatusInfo> ConnectedNodes { get; set; }
    }
}