namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class Node : Base
    {
        public string ContactPoint { get; set; }
        public Node(string id) : base(id)
        {
        }
    }
}