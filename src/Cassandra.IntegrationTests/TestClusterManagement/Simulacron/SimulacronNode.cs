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
    }
}