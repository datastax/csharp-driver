//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Test.Integration.TestClusterManagement
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
        /// <param name="start">If the node should be started</param>
        void BootstrapNode(int nodeIdToStart, bool start = true);

        /// <summary>
        /// Set workload of a single node
        /// </summary>
        /// <param name="nodeId">The node ID to be added to the cluster</param>
        /// <param name="workloads">The node workloads</param>
        void SetNodeWorkloads(int nodeId, string[] workloads);

        /// <summary>
        /// Bootstraps and adds a node to the cluster
        /// </summary>
        /// <param name="nodeIdToStart"></param>
        /// <param name="dataCenterName"></param>
        /// <param name="start">If the node should be started</param>
        void BootstrapNode(int nodeIdToStart, string dataCenterName, bool start = true);

        /// <summary>
        /// Decommission the node associated with provided node ID
        /// </summary>
        /// <param name="nodeId">The node ID to be decommissioned</param>
        void DecommissionNode(int nodeId);

        /// <summary>
        /// Forcefully decommission the node associated with provided node ID
        /// </summary>
        /// <param name="nodeId">The node ID to be decommissioned</param>
        void DecommissionNodeForcefully(int nodeId);

        /// <summary>
        /// Pause the node (SIGSTOP) associated with provided node ID
        /// </summary>
        void PauseNode(int nodeId);

        /// <summary>
        /// Resumes the node (SIGCONT) associated with provided node ID
        /// </summary>
        void ResumeNode(int nodeId);
    }

    public class TestClusterOptions
    {
        public static readonly TestClusterOptions Default = new TestClusterOptions();

        public bool UseVNodes { get; set; }

        public bool UseSsl { get; set; }

        public string[] CassandraYaml { get; set; }

        public int Dc2NodeLength { get; set; }

        public string[] JvmArgs { get; set; }

        /// <summary>
        /// DSE yaml options
        /// </summary>
        public string[] DseYaml { get; set; }

        /// <summary>
        /// DSE Nodes workloads
        /// </summary>
        public string[] Workloads { get; set; }
    }
}
