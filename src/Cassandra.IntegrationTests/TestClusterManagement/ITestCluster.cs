using System.Collections.Generic;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public interface ITestCluster
    {
        string Name { get; set; }
        Builder Builder { get; set; }
        Cluster Cluster { get; set; }
        ISession Session { get; set; }
        int Dc1NodeCount { get; set; }
        int Dc2NodeCount { get; set; }
        string InitialContactPoint { get; set; }
        string ClusterIpPrefix { get; set; }
        string DefaultKeyspace { get; set; }
        bool IsBeingCreated { get; set; }
        bool IsCreated { get; set; }
        bool IsStarted { get; set; }
        bool IsUsingDefaultConfig { get; set; }
        bool IsRemoved { get; set; }
        List<string> ExpectedInitialHosts { get; set; }

        /// <summary>
        /// Stops all clients and Cassandra nodes.
        /// </summary>
        void ShutDown();

        /// <summary>
        /// Removes all Cassandra nodes.
        /// </summary>
        void Remove();

        /// <summary>
        /// Waits for the cluster to be initialized and available to handle requests
        /// </summary>
        void SwitchToThisAndStart();

        /// <summary>
        /// Creates the cluster with the option of starting it as well
        /// </summary>
        void Create(bool startCluster = true, string[] jvmArgs = null);

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

        /// <summary>
        /// Puts focus on this cluster
        /// This is relevant for CCM, all other tools should be a no-op
        /// </summary>
        void SwitchToThisCluster();

        void UseVNodes(string nodesToPopulate);
    }
}
