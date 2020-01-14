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
using System.Diagnostics;
using System.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class CcmCluster : ITestCluster
    {
        public string Name { get; set; }
        public string Version { get; set; }
        public Builder Builder { get; set; }
        public Cluster Cluster { get; set; }
        public ISession Session { get; set; }
        public string InitialContactPoint { get; set; }
        public string ClusterIpPrefix { get; set; }
        public string DsePath { get; set; }
        public string DefaultKeyspace { get; set; }
        private readonly ICcmProcessExecuter _executor;
        private CcmBridge _ccm;

        public CcmCluster(string name, string clusterIpPrefix, string dsePath, ICcmProcessExecuter executor, string defaultKeyspace, string version)
        {
            _executor = executor;
            Name = name;
            DefaultKeyspace = defaultKeyspace;
            ClusterIpPrefix = clusterIpPrefix;
            DsePath = dsePath;
            InitialContactPoint = ClusterIpPrefix + "1";
            Version = version;
        }

        public void Create(int nodeLength, TestClusterOptions options = null)
        {
            options = options ?? TestClusterOptions.Default;
            _ccm = new CcmBridge(Name, ClusterIpPrefix, DsePath, Version, _executor);
            _ccm.Create(options.UseSsl);
            _ccm.Populate(nodeLength, options.Dc2NodeLength, options.UseVNodes);
            _ccm.UpdateConfig(options.CassandraYaml);

            if (TestClusterManager.IsDse)
            {
                _ccm.UpdateDseConfig(options.DseYaml);
                _ccm.SetWorkloads(nodeLength, options.Workloads);
            }
        }

        public void InitClient()
        {
            Cluster?.Shutdown();
            if (Builder == null)
            {
                Builder = new Builder();   
            }
            Cluster = Builder.AddContactPoint(InitialContactPoint).Build();
            Session = Cluster.Connect();
            if (DefaultKeyspace != null)
            {
                Session.CreateKeyspaceIfNotExists(DefaultKeyspace);
                Session.ChangeKeyspace(DefaultKeyspace);   
            }
        }

        public void ShutDown()
        {
            Cluster?.Shutdown();
            _ccm.Stop();
        }

        public void Remove()
        {
            Trace.TraceInformation($"Removing Cluster with Name: '{Name}', InitialContactPoint: {InitialContactPoint}, and CcmDir: {_ccm.CcmDir}");
            _ccm.Remove();
        }

        public void Remove(int nodeId)
        {
            Trace.TraceInformation($"Removing node '{nodeId}' from cluster '{Name}'");
            _ccm.Remove(nodeId);
        }

        public void DecommissionNode(int nodeId)
        {
            _ccm.DecommissionNode(nodeId);
        }

        public void DecommissionNodeForcefully(int nodeId)
        {
            _ccm.ExecuteCcm(string.Format("node{0} nodetool \"decommission -f\"", nodeId));
        }

        public void PauseNode(int nodeId)
        {
            _ccm.ExecuteCcm($"node{nodeId} pause");
        }

        public void ResumeNode(int nodeId)
        {
            _ccm.ExecuteCcm($"node{nodeId} resume");
        }

        public void SwitchToThisCluster()
        {
            _ccm.SwitchToThis();
        }

        public void StopForce(int nodeIdToStop)
        {
            _ccm.StopForce(nodeIdToStop);
        }

        public void Stop(int nodeIdToStop)
        {
            _ccm.Stop(nodeIdToStop);
        }

        public void Start(string[] jvmArgs = null)
        {
            _ccm.Start(jvmArgs);
        }

        public void Start(int nodeIdToStart, string additionalArgs = null)
        {
            _ccm.Start(nodeIdToStart, additionalArgs);
        }

        public void BootstrapNode(int nodeIdToStart, bool start = true)
        {
            _ccm.BootstrapNode(nodeIdToStart, start);
        }

        public void SetNodeWorkloads(int nodeId, string[] workloads)
        {
            if (!TestClusterManager.IsDse)
            {
                throw new InvalidOperationException("Cant set workloads on an oss cluster.");
            }

            _ccm.SetNodeWorkloads(nodeId, workloads);
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName, bool start = true)
        {
            _ccm.BootstrapNode(nodeIdToStart, dataCenterName, start);
        }

        public void UpdateConfig(params string[] yamlChanges)
        {
            if (yamlChanges == null) return;
            var joinedChanges = string.Join(" ", yamlChanges.Select(s => $"\"{s}\""));
            _ccm.ExecuteCcm($"updateconf {joinedChanges}");
        }

        public void UpdateConfig(int nodeId, params string[] yamlChanges)
        {
            if (yamlChanges == null) return;
            var joinedChanges = string.Join(" ", yamlChanges.Select(s => $"\"{s}\""));
            _ccm.ExecuteCcm($"node{nodeId} updateconf {joinedChanges}");
        }
    }
}
