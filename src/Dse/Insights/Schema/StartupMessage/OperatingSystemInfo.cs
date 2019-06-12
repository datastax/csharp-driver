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
    internal class OperatingSystemInfo
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("version")]
        public string Version { get; set; }

        [JsonProperty("arch")]
        public string Arch { get; set; }
    }
}