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
    internal class PoolSizeByHostDistance
    {
        [JsonProperty("local")]
        public int Local { get; set; }

        [JsonProperty("remote")]
        public int Remote { get; set; }
    }
}