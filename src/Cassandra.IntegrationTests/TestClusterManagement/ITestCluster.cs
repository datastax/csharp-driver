using System;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public interface ITestCluster
    {
        string Name { get; set; }
        Builder Builder { get; set; }
        Cluster Cluster { get; set; }
        ISession Session { get; set; }
        string InitialContactPoint { get; set; }
        string ClusterIpPrefix { get; set; }
        string DefaultKeyspace { get; set; }

        /// <summary>
        /// Stops all clients and Cassandra nodes.
        /// </summary>
        void ShutDown();

        /// <summary>
        /// Removes all Cassandra nodes.
        /// </summary>
        void Remove();

        /// <summary>
        /// Removes specific node from Cassandra cluster.
        /// </summary>
        void Remove(int nodeId);

        /// <summary>
        /// Creates the cluster with the option provided
        /// </summary>
        void Create(int nodeLength, TestClusterOptions options = null);

        /// <summary>
        /// Force Stop a specific node in the cluster
        /// </summary>
        void StopForce(int nodeIdToStop);

        /// <summary>
        /// Stop a specific node in the cluster
        /// </summary>
        void Stop(int nodeIdToStop);

        /// <summary>
        /// Start a specific node in the cluster
        /// </summary>
        void Start(int nodeIdToStart, string additionalArgs = null);

        /// <summary>
        /// Starts the cluster
        /// </summary>
        void Start(string[] jvmArgs = null);

        /// <summary>
        /// Updates the yaml config
        /// </summary>
        void UpdateConfig(params string[] yamlChanges);

        /// <summary>
        /// Initialize the Builder, Cluster and Session objects associated with the current Test Cluster
        /// </summary>
        void InitClient();

        /// <summary>
        /// Bootstraps and adds a node to the cluster
        /// </summary>
        /// <param name="nodeIdToStart">The node ID to be added to the cluster</param>
        void BootstrapNode(int nodeIdToStart);

        /// <summary>
        /// Bootstraps and adds a node to the cluster
        /// </summary>
        /// <param name="nodeIdToStart"></param>
        /// <param name="dataCenterName"></param>
        void BootstrapNode(int nodeIdToStart, string dataCenterName);

        /// <summary>
        /// Decommission the node associated with provided node ID
        /// </summary>
        /// <param name="nodeId">The node ID to be decommissioned</param>
        void DecommissionNode(int nodeId);

        /// <summary>
        /// Pause the node (SIGSTOP) associated with provided node ID
        /// </summary>
        void PauseNode(int nodeId);

        /// <summary>
        /// Resumes the node (SIGCONT) associated with provided node ID
        /// </summary>
        void ResumeNode(int nodeId);
    }

    public class TestClusterOptions : IEquatable<TestClusterOptions>
    {
        public static readonly TestClusterOptions Default = new TestClusterOptions();

        public bool UseVNodes { get; set; }

        public bool UseSsl { get; set; }

        public string[] CassandraYaml { get; set; }

        public int Dc2NodeLength { get; set; }

        public string[] JvmArgs { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as TestClusterOptions);
        }

        public bool Equals(TestClusterOptions other)
        {
            return other != null &&
                   UseVNodes == other.UseVNodes &&
                   UseSsl == other.UseSsl &&
                   CassandraYaml.OrderBy(i => i).SequenceEqual(other.CassandraYaml.OrderBy(i => i)) &&
                   Dc2NodeLength == other.Dc2NodeLength &&
                   JvmArgs.OrderBy(i => i).SequenceEqual(other.JvmArgs.OrderBy(i => i));
        }

        public override int GetHashCode()
        {
            var hashCode = 651983754;
            hashCode = hashCode * -1521134295 + UseVNodes.GetHashCode();
            hashCode = hashCode * -1521134295 + UseSsl.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<string[]>.Default.GetHashCode(CassandraYaml);
            hashCode = hashCode * -1521134295 + Dc2NodeLength.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<string[]>.Default.GetHashCode(JvmArgs);
            return hashCode;
        }
    }
}