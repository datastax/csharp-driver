//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public interface ITestCluster
    {
        string Name { get; set; }
        string Version { get; set; }
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
        void Start(int nodeIdToStart, string additionalArgs = null, string newIp = null);

        /// <summary>
        /// Starts the cluster
        /// </summary>
        void Start(string[] jvmArgs = null);
        
        /// <summary>
        /// Updates the dse yaml config
        /// </summary>
        void UpdateDseConfig(params string[] yamlChanges);

        /// <summary>
        /// Updates the yaml config
        /// </summary>
        void UpdateConfig(params string[] yamlChanges);
        
        /// <summary>
        /// Updates the yaml config of a specific node
        /// </summary>
        void UpdateConfig(int nodeId, params string[] yamlChanges);

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

        void SwitchToThisCluster();
    }

    public class TestClusterOptions : IEquatable<TestClusterOptions>
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
                   JvmArgs.OrderBy(i => i).SequenceEqual(other.JvmArgs.OrderBy(i => i)) &&
                   DseYaml.OrderBy(i => i).SequenceEqual(other.DseYaml.OrderBy(i => i)) &&
                   Workloads.OrderBy(i => i).SequenceEqual(other.Workloads.OrderBy(i => i));
        }

        public override int GetHashCode()
        {
            var hashCode = 651983754;
            hashCode = hashCode * -1521134295 + UseVNodes.GetHashCode();
            hashCode = hashCode * -1521134295 + UseSsl.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<string[]>.Default.GetHashCode(CassandraYaml);
            hashCode = hashCode * -1521134295 + Dc2NodeLength.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<string[]>.Default.GetHashCode(JvmArgs);
            hashCode = hashCode * -1521134295 + EqualityComparer<string[]>.Default.GetHashCode(DseYaml);
            hashCode = hashCode * -1521134295 + EqualityComparer<string[]>.Default.GetHashCode(Workloads);
            return hashCode;
        }
    }
}