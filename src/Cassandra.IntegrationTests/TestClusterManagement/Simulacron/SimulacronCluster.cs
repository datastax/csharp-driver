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

        public SimulacronCluster(string id, SimulacronManager simulacronManager) : base(id, simulacronManager)
        {
        }
        
        public static Task<SimulacronCluster> CreateNewAsync(int nodeLength)
        {
            return SimulacronManager.DefaultInstance.CreateNewAsync(nodeLength);
        }

        /// <summary>
        /// Creates a single DC cluster with the amount of nodes provided.
        /// </summary>
        public static SimulacronCluster CreateNew(int nodeLength)
        {
            return SimulacronManager.DefaultInstance.CreateNew(nodeLength);
        }
        
        public static Task<SimulacronCluster> CreateNewAsync(SimulacronOptions options)
        {
            return SimulacronManager.DefaultInstance.CreateNewAsync(options);
        }

        public static SimulacronCluster CreateNew(SimulacronOptions options)
        {
            return SimulacronManager.DefaultInstance.CreateNew(options);
        }

        /// <summary>
        /// Creates a new cluster with POST body parameters.
        /// </summary>
        public static SimulacronCluster CreateNewWithPostBody(dynamic body)
        {
            return SimulacronManager.DefaultInstance.CreateNewWithPostBody(body);
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