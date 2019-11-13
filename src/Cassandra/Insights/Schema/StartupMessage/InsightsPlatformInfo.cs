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
    internal class InsightsPlatformInfo
    {
        [JsonProperty("os")]
        public OperatingSystemInfo OperatingSystem { get; set; }

        [JsonProperty("cpus")]
        public CentralProcessingUnitsInfo CentralProcessingUnits { get; set; }

        [JsonProperty("runtime")]
        public RuntimeInfo Runtime { get; set; }
    }
}