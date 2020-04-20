//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronCluster : SimulacronBase, IDisposable
    {
        public dynamic Data { get; set; }
        public List<SimulacronDataCenter> DataCenters { get; set; }
        private const string CreateClusterPathFormat = "/cluster?data_centers={0}&cassandra_version={1}&dse_version={2}&name={3}" +
                                                       "&activity_log={4}&num_tokens={5}";
        private const string CreateClusterPath = "/cluster";

        public IPEndPoint InitialContactPoint
        {
            get
            {
                var contact = DataCenters.First().Nodes.First().ContactPoint;
                return GetTupleFromContactPoint(contact);
            }
        }

        public IEnumerable<IPEndPoint> ContactPoints
        {
            get { return DataCenters.SelectMany(d => d.Nodes).Select(n => GetTupleFromContactPoint(n.ContactPoint)); }
        }

        private IPEndPoint GetTupleFromContactPoint(string contact)
        {
            if (contact.Contains(":"))
            {
                var parts = contact.Split(':');
                var addr = parts[0];
                var port = int.Parse(parts[1]);
                return new IPEndPoint(IPAddress.Parse(addr), port);
            }
            return new IPEndPoint(IPAddress.Parse(contact), 9042);
        }

        private SimulacronCluster(string id, SimulacronManager simulacronManager) : base(id, simulacronManager)
        {
        }
        
        public static Task<SimulacronCluster> CreateNewAsync(int nodeLength)
        {
            return CreateNewAsync(new SimulacronOptions { Nodes = nodeLength.ToString() });
        }

        /// <summary>
        /// Creates a single DC cluster with the amount of nodes provided.
        /// </summary>
        public static SimulacronCluster CreateNew(int nodeLength)
        {
            return CreateNew(new SimulacronOptions { Nodes = nodeLength.ToString() });
        }
        
        public static Task<SimulacronCluster> CreateNewAsync(SimulacronOptions options)
        {
            SimulacronManager.DefaultInstance.Start();
            return CreateNewAsync(SimulacronManager.DefaultInstance, options);
        }
        
        public static async Task<SimulacronCluster> CreateNewAsync(SimulacronManager simulacronManager, SimulacronOptions options)
        {
            var path = string.Format(CreateClusterPathFormat, options.Nodes, options.GetCassandraVersion(),
                options.GetDseVersion(), options.Name, options.ActivityLog, options.NumberOfTokens);
            var data = await Post(simulacronManager, path, null).ConfigureAwait(false);
            return CreateFromData(simulacronManager, data);
        }

        public static SimulacronCluster CreateNew(SimulacronOptions options)
        {
            return TaskHelper.WaitToComplete(CreateNewAsync(options));
        }

        public static SimulacronCluster CreateNew(SimulacronManager simulacronManager, SimulacronOptions options)
        {
            return TaskHelper.WaitToComplete(CreateNewAsync(simulacronManager, options));
        }
        
        /// <summary>
        /// Creates a new cluster with POST body parameters.
        /// </summary>
        public static SimulacronCluster CreateNewWithPostBody(dynamic body)
        {
            var simulacronManager = SimulacronManager.DefaultInstance;
            simulacronManager.Start();
            return CreateNewWithPostBody(simulacronManager, body);
        }
        
        /// <summary>
        /// Creates a new cluster with POST body parameters.
        /// </summary>
        public static SimulacronCluster CreateNewWithPostBody(SimulacronManager simulacronManager, dynamic body)
        {
            var data = TaskHelper.WaitToComplete(Post(simulacronManager, CreateClusterPath, body));
            return CreateFromData(simulacronManager, data);
        }

        private static SimulacronCluster CreateFromData(SimulacronManager simulacronManager, dynamic data)
        {
            var cluster = new SimulacronCluster(data["id"].ToString(), simulacronManager)
            {
                Data = data,
                DataCenters = new List<SimulacronDataCenter>()
            };
            var dcs = (JArray) cluster.Data["data_centers"];
            foreach (var dc in dcs)
            {
                var dataCenter = new SimulacronDataCenter(cluster.Id + "/" + dc["id"], simulacronManager);
                cluster.DataCenters.Add(dataCenter);
                dataCenter.Nodes = new List<SimulacronNode>();
                var nodes = (JArray) dc["nodes"];
                foreach (var nodeJObject in nodes)
                {
                    var node = new SimulacronNode(dataCenter.Id + "/" + nodeJObject["id"], simulacronManager);
                    dataCenter.Nodes.Add(node);
                    node.ContactPoint = nodeJObject["address"].ToString();
                }
            }
            return cluster;
        }

        public Task DropConnection(string ip, int port)
        {
            return DeleteAsync(GetPath("connection") + "/" + ip + "/" + port);
        }

        public Task DropConnection(IPEndPoint endpoint)
        {
            return DropConnection(endpoint.Address.ToString(), endpoint.Port);
        }

        public async Task<List<IPEndPoint>> GetConnectedPortsAsync()
        {
            var result = new List<IPEndPoint>();
            var response = await GetConnectionsAsync().ConfigureAwait(false);
            var dcs = (JArray) response["data_centers"];
            foreach (var dc in dcs)
            {
                var nodes = (JArray) dc["nodes"];
                foreach (var nodeJObject in nodes)
                {
                    var connections = (JArray) nodeJObject["connections"];
                    foreach (var conn in connections)
                    {
                        result.Add(GetTupleFromContactPoint(conn.ToString()));
                    }
                }
            }
            return result;
        }

        public async Task RemoveAsync()
        {
            await DeleteAsync(GetPath("cluster")).ConfigureAwait(false);
        }

        public SimulacronNode GetNode(int index)
        {
            return DataCenters.SelectMany(dc => dc.Nodes).ElementAt(index);
        }

        public SimulacronNode GetNode(string endpoint)
        {
            return DataCenters.SelectMany(dc => dc.Nodes).FirstOrDefault(n => n.ContactPoint == endpoint);
        }

        public SimulacronNode GetNode(IPEndPoint endpoint)
        {
            return GetNode(endpoint.ToString());
        }

        public IEnumerable<SimulacronNode> GetNodes()
        {
            return DataCenters.SelectMany(dc => dc.Nodes);
        }

        public void Dispose()
        {
            TaskHelper.WaitToComplete(Task.Run(ShutDownAsync), 60 * 1000);
        }

        public Task ShutDownAsync()
        {
            return RemoveAsync();
        }
    }
}