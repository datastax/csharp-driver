//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Newtonsoft.Json;

namespace Cassandra.Insights.Schema.StartupMessage
{
    [JsonObject]
    internal class AuthProviderInfo
    {
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("namespace")]
        public string Namespace { get; set; }
    }
}