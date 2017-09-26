using System.Collections.Generic;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronDataCenter : SimulacronBase
    {
        public List<SimulacronNode> Nodes { get; set; }
        public SimulacronDataCenter(string id): base(id)
        {
        }
    }
}