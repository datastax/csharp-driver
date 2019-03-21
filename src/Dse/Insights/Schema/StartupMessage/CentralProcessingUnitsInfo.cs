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
    internal class CentralProcessingUnitsInfo
    {
        [JsonProperty("length")]
        public int Length { get; set; }

        [JsonProperty("model")]
        public string Model { get; set; }
    }
}