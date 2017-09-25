// //
// //      Copyright (C) 2017 DataStax Inc.
// //
// //   Licensed under the Apache License, Version 2.0 (the "License");
// //   you may not use this file except in compliance with the License.
// //   You may obtain a copy of the License at
// //
// //      http://www.apache.org/licenses/LICENSE-2.0
// //
// //   Unless required by applicable law or agreed to in writing, software
// //   distributed under the License is distributed on an "AS IS" BASIS,
// //   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// //   See the License for the specific language governing permissions and
// //   limitations under the License.
// //

using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class Cluster : Base
    {
        public dynamic Data { get; set; }
        public List<DataCenter> DataCenters { get; set; }

        public Tuple<string, int> InitialContactPoint
        {
            get
            {
                var contact = DataCenters.First().Nodes.First().ContactPoint;
                if (contact.Contains(":"))
                {
                    var parts = contact.Split(':');
                    var addr = parts[0];
                    var port = int.Parse(parts[1]);
                    return new Tuple<string, int>(addr, port);
                }
                return new Tuple<string, int>(contact, 9042);
            }
        }

        private const string CreateClusterPathFormat = "/cluster?data_centers={0}&cassandra_version={1}&dse_version={2}&name={3}&activity_log={4}&num_tokens={5}";
        private Cluster()
        {
        }

        public static Cluster Create(string dcNodes, string version, string name, bool activityLog, int numTokens, bool dse = false)
        {
            var path = string.Format(CreateClusterPathFormat, dcNodes, (!dse ? version : ""), (dse ? version : ""), name, activityLog, numTokens);
            var cluster = new Cluster();
            cluster.BaseAddress = SimulacronManager.BaseAddress;
            cluster.Data = cluster.Post(path, null).Result;
            cluster.Id = cluster.Data["id"];
            cluster.DataCenters = new List<DataCenter>();
            var dcs = (JArray) cluster.Data["data_centers"];
            foreach (var dc in dcs)
            {
                var dataCenter = new DataCenter(cluster.Id + "/" + dc["id"]);
                cluster.DataCenters.Add(dataCenter);
                dataCenter.BaseAddress = cluster.BaseAddress;
                dataCenter.Nodes = new List<Node>();
                var nodes = (JArray) dc["nodes"];
                foreach (var nodeJObject in nodes)
                {
                    var node = new Node(dataCenter.Id + "/" + nodeJObject["id"]);
                    dataCenter.Nodes.Add(node);
                    node.BaseAddress = cluster.BaseAddress;
                    node.ContactPoint = nodeJObject["address"].ToString();
                }
            }
            return cluster;
        }
    }
}