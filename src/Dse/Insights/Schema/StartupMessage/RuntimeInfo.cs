//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Dse.Insights.Schema.StartupMessage
{
    [JsonObject]
    internal class RuntimeInfo
    {
        [JsonProperty("runtimeFramework")]
        public string RuntimeFramework { get; set; }

        [JsonProperty("targetFramework")]
        public string TargetFramework { get; set; }

        [JsonProperty("dependencies")]
        public Dictionary<string, AssemblyInfo> Dependencies { get; set; }
    }
}