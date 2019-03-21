//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Newtonsoft.Json;

namespace Dse.Insights.Schema.StartupMessage
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