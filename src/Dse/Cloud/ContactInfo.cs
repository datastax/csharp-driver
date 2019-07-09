// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Dse.Cloud
{
    [JsonObject]
    internal class ContactInfo
    {
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("local_dc")]
        public string LocalDc { get; set; }

        [JsonRequired]
        [JsonProperty("contact_points")]
        public List<string> ContactPoints { get; set; }
        
        [JsonRequired]
        [JsonProperty("sni_proxy_address")]
        public string SniProxyAddress { get; set; }
    }
}