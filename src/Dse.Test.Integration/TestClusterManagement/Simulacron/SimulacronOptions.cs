//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Test.Integration.TestClusterManagement.Simulacron
{
    public class SimulacronOptions
    {
        public string Nodes { get; set; }

        public string Version { get; set; }

        public string DseVersion { get; set; }

        public string Name { get; set; }

        public bool ActivityLog { get; set; }

        public int NumberOfTokens { get; set; }

        public bool IsDse { get; set; }

        public SimulacronOptions()
        {
            Nodes = "1";
            Version = TestClusterManager.CassandraVersion.ToString();
            DseVersion = TestClusterManager.DseVersion?.ToString();
            Name = TestUtils.GetTestClusterNameBasedOnTime();
            ActivityLog = true;
            NumberOfTokens = 1;
            IsDse = false;
        }

        public string GetCassandraVersion()
        {
            return Version;
        }

        public string GetDseVersion()
        {
            return !IsDse || DseVersion == null ? string.Empty : DseVersion;
        }
    }
}