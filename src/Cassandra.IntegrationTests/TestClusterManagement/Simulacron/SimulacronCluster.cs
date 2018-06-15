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

        private SimulacronCluster(string id) : base(id)
        {
        }

        /// <summary>
        /// Creates a single DC cluster with the amount of nodes provided.
        /// </summary>
        public static SimulacronCluster CreateNew(int nodeLength)
        {
            return CreateNew(new SimulacronOptions { Nodes = nodeLength.ToString() });
        }

        public static SimulacronCluster CreateNew(SimulacronOptions options)
        {
            var simulacronManager = SimulacronManager.Instance;
            simulacronManager.Start();
            var path = string.Format(CreateClusterPathFormat, options.Nodes, options.GetCassandraVersion(),
                options.GetDseVersion(), options.Name, options.ActivityLog, options.NumberOfTokens);
            var data = TaskHelper.WaitToComplete(Post(path, null));
            return CreateFromData(data);
        }

        /// <summary>
        /// Creates a new cluster with POST body parameters.
        /// </summary>
        public static SimulacronCluster CreateNewWithPostBody(dynamic body)
        {
            var simulacronManager = SimulacronManager.Instance;
            simulacronManager.Start();
            var data = TaskHelper.WaitToComplete(Post(CreateClusterPath, body));
            return CreateFromData(data);
        }

        private static SimulacronCluster CreateFromData(dynamic data)
        {
            var cluster = new SimulacronCluster(data["id"].ToString())
            {
                Data = data,
                DataCenters = new List<SimulacronDataCenter>()
            };
            var dcs = (JArray) cluster.Data["data_centers"];
            foreach (var dc in dcs)
            {
                var dataCenter = new SimulacronDataCenter(cluster.Id + "/" + dc["id"]);
                cluster.DataCenters.Add(dataCenter);
                dataCenter.Nodes = new List<SimulacronNode>();
                var nodes = (JArray) dc["nodes"];
                foreach (var nodeJObject in nodes)
                {
                    var node = new SimulacronNode(dataCenter.Id + "/" + nodeJObject["id"]);
                    dataCenter.Nodes.Add(node);
                    node.ContactPoint = nodeJObject["address"].ToString();
                }
            }
            return cluster;
        }

        public Task DropConnection(string ip, int port)
        {
            return Delete(GetPath("connection") + "/" + ip + "/" + port);
        }

        public Task DropConnection(IPEndPoint endpoint)
        {
            return DropConnection(endpoint.Address.ToString(), endpoint.Port);
        }

        public List<IPEndPoint> GetConnectedPorts()
        {
            var result = new List<IPEndPoint>();
            var response = GetConnections();
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

        public Task Remove()
        {
            return Delete(GetPath("cluster"));
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
            TaskHelper.WaitToComplete(Remove());
        }
    }
}