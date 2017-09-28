using System;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronOptions
    {
        public string Nodes { get; set; }

        public string Version { get; set; }

        public string Name { get; set; }

        public bool ActivityLog { get; set; }

        public int NumberOfTokens { get; set; }

        public bool IsDse { get; set; }

        public SimulacronOptions()
        {
            Nodes = "1";
            Version = TestClusterManager.CassandraVersionText;
            Name = TestUtils.GetTestClusterNameBasedOnTime();
            ActivityLog = true;
            NumberOfTokens = 1;
            IsDse = false;
        }

        public string GetCassandraVersion()
        {
            return IsDse ? string.Empty : Version;
        }

        public string GetDseVersion()
        {
            return !IsDse ? string.Empty : Version;
        }
    }
}