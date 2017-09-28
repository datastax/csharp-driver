namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronNode : SimulacronBase
    {
        public string ContactPoint { get; set; }
        public SimulacronNode(string id) : base(id)
        {
        }
    }
}