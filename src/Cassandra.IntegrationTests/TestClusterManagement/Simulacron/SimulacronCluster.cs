﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronCluster : SimulacronBase
    {
        public dynamic Data { get; set; }
        public List<SimulacronDataCenter> DataCenters { get; set; }
        private const string CreateClusterPathFormat = "/cluster?data_centers={0}&cassandra_version={1}&dse_version={2}&name={3}" +
                                                       "&activity_log={4}&num_tokens={5}";

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

        public static SimulacronCluster CreateNew(SimulacronOptions options)
        {
            var simulacronManager = SimulacronManager.Instance;
            if (!simulacronManager.IsUp())
            {
                simulacronManager.Start();
            }
            var path = string.Format(CreateClusterPathFormat, options.Nodes, options.GetCassandraVersion(), options.GetDseVersion(), options.Name, 
                options.ActivityLog, options.NumberOfTokens);
            var data = Post(path, null).Result;
            var cluster = new SimulacronCluster(data["id"].ToString());
            cluster.Data = data;
            cluster.DataCenters = new List<SimulacronDataCenter>();
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
    }
}