using System.Collections.Generic;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class DataCenter : Base
    {
        public List<Node> Nodes { get; set; }
        public DataCenter(string id): base(id)
        {
        }
    }
}