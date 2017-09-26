using System;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronOptions
    {
//        string dcNodes, string version, string name, bool activityLog, int numTokens, bool dse = false
        public string Nodes { get; set; }

        public string Version { get; set; }

        public string Name { get; set; }

        public bool ActivityLog { get; set; }

        public int NumberOfTokens { get; set; }

        public bool IsDse { get; set; }

        public static SimulacronOptions GetDefaultOptions()
        {
            return new SimulacronOptions
            {
                Nodes = "1",
                Version = TestClusterManager.CassandraVersionText,
                Name = TestUtils.GetTestClusterNameBasedOnTime(),
                ActivityLog = true,
                NumberOfTokens = 1,
                IsDse = false
            };
        }

        public string GetCassandraVersion()
        {
            if (IsDse)
            {
                return string.Empty;
            }
            return Version;
        }

        public string GetDseVersion()
        {
            if (!IsDse)
            {
                return string.Empty;
            }
            return Version;
        }
    }
}