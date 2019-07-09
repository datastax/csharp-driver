//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Newtonsoft.Json;

namespace Dse.Cloud
{
    [JsonObject]
    internal class CloudMetadataResult
    {
        [JsonProperty("region")]
        public string Region { get; set; }

        [JsonRequired]
        [JsonProperty("contact_info")]
        public ContactInfo ContactInfo { get; set; }
    }
}