using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronNode : SimulacronBase
    {
        public string ContactPoint { get; set; }
        
        public SimulacronNode(string id) : base(id)
        {

        }

        public Task Stop()
        {
            return Delete($"/listener/{Id}?type=stop");
        }

        public Task Start()
        {
            return Put($"/listener/{Id}", null);
        }

        /// <summary>
        /// Gets the list of established connections to a node.
        /// </summary>
        public new IList<IPEndPoint> GetConnections()
        {
            var nodeInfo = base.GetConnections();
            IEnumerable connections = nodeInfo["data_centers"][0]["nodes"][0]["connections"];

            return (from object element in connections
                    select element.ToString().Split(':')
                    into parts
                    select new IPEndPoint(IPAddress.Parse(parts[0]), Convert.ToInt32(parts[1]))).ToList();
        }
    }
}