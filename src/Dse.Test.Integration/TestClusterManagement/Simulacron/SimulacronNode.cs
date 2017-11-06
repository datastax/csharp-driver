using System.Threading.Tasks;

namespace Dse.Test.Integration.TestClusterManagement.Simulacron
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