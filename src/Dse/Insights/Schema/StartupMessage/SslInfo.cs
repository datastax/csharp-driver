//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Newtonsoft.Json;

namespace Dse.Insights.Schema.StartupMessage
{
    [JsonObject]
    internal class SslInfo
    {
        [JsonProperty("enabled")]
        public bool Enabled { get; set; }
    }
}