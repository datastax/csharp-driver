using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class Cluster : Base
    {
        public dynamic Data { get; set; }
        public List<DataCenter> DataCenters { get; set; }
        private const string CreateClusterPathFormat = "/cluster?data_centers={0}&cassandra_version={1}&dse_version={2}&name={3}" +
                                                       "&activity_log={4}&num_tokens={5}";

        public Tuple<string, int> InitialContactPoint
        {
            get
            {
                var contact = DataCenters.First().Nodes.First().ContactPoint;
                return GetTupleFromContactPoint(contact);
            }
        }

        private Tuple<string, int> GetTupleFromContactPoint(string contact)
        {
            if (contact.Contains(":"))
            {
                var parts = contact.Split(':');
                var addr = parts[0];
                var port = int.Parse(parts[1]);
                return new Tuple<string, int>(addr, port);
            }
            return new Tuple<string, int>(contact, 9042);
        }

        private Cluster(string id) : base(id)
        {
        }

        public static Cluster Create(string dcNodes, string version, string name, bool activityLog, int numTokens, bool dse = false)
        {
            var path = string.Format(CreateClusterPathFormat, dcNodes, (!dse ? version : ""), (dse ? version : ""), name, activityLog, numTokens);
            var data = Post(path, null).Result;
            var cluster = new Cluster(data["id"].ToString());
            cluster.Data = data;
            cluster.DataCenters = new List<DataCenter>();
            var dcs = (JArray) cluster.Data["data_centers"];
            foreach (var dc in dcs)
            {
                var dataCenter = new DataCenter(cluster.Id + "/" + dc["id"]);
                cluster.DataCenters.Add(dataCenter);
                dataCenter.Nodes = new List<Node>();
                var nodes = (JArray) dc["nodes"];
                foreach (var nodeJObject in nodes)
                {
                    var node = new Node(dataCenter.Id + "/" + nodeJObject["id"]);
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

        public List<Tuple<string, int>> GetConnectedPorts()
        {
            var result = new List<Tuple<string, int>>();
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
    }
}