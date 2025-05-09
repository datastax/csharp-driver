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
using Cassandra.IntegrationTests.TestBase;

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
        public string IdPrefix { get; private set; }
        public string DsePath { get; set; }
        public string DefaultKeyspace { get; set; }
        private readonly ICcmProcessExecuter _executor;
        private CcmBridge _ccm;
        private int _nodeLength;

        public CcmCluster(string name, string idPrefix, string dsePath, ICcmProcessExecuter executor, string defaultKeyspace, string version)
        {
            _executor = executor;
            Name = name;
            DefaultKeyspace = defaultKeyspace;
            IdPrefix = idPrefix;
            ClusterIpPrefix = $"127.0.{IdPrefix}.";
            InitialContactPoint = ClusterIpPrefix + "1";
            DsePath = dsePath;
            Version = version;
        }

        public void Create(int nodeLength, TestClusterOptions options = null)
        {
            _nodeLength = nodeLength;
            options = options ?? TestClusterOptions.Default;
            _ccm = new CcmBridge(Name, IdPrefix, DsePath, Version, _executor);
            _ccm.Create(options.UseSsl);
            _ccm.Populate(nodeLength, options.Dc2NodeLength, options.UseVNodes);
            _ccm.UpdateConfig(options.CassandraYaml);

            if (TestClusterManager.IsDse)
            {
                _ccm.UpdateDseConfig(options.DseYaml);
                _ccm.SetWorkloads(nodeLength, options.Workloads);
            }

            if (TestClusterManager.Executor is WslCcmProcessExecuter)
            {
                _ccm.UpdateConfig(new[]
                {
                    "read_request_timeout_in_ms: 20000",
                    "counter_write_request_timeout_in_ms: 20000",
                    "write_request_timeout_in_ms: 20000",
                    "request_timeout_in_ms: 20000",
                    "range_request_timeout_in_ms: 30000"
                });
                if (TestClusterManager.IsDse)
                {
                    if (TestClusterManager.CheckDseVersion(new Version(6, 7), Comparison.LessThan))
                    {
                        _ccm.UpdateConfig(new[]
                        {
                            "user_defined_function_fail_timeout: 20000"
                        });
                    }
                    else
                    {
                        _ccm.UpdateConfig(new[]
                        {
                            "user_defined_function_fail_micros: 20000"
                        });
                    }
                }
            }
        }

        public void InitClient()
        {
            Cluster?.Shutdown();
            if (Builder == null)
            {
                Builder = TestUtils.NewBuilder();
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
            _ccm.ExecuteCcm(string.Format("node{0} nodetool \"decommission -f\"", nodeId), false);
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
            var output = _ccm.Start(jvmArgs);
            if (_executor is WslCcmProcessExecuter)
            {
                foreach (var i in Enumerable.Range(1, _nodeLength))
                {
                    _ccm.CheckNativePortOpen(output, TestClusterManager.IpPrefix + i);
                }
            }
        }

        public void Start(int nodeIdToStart, string additionalArgs = null, string newIp = null, string[] jvmArgs = null)
        {
            var output = _ccm.Start(nodeIdToStart, additionalArgs, jvmArgs);
            if (_executor is WslCcmProcessExecuter)
            {
                _ccm.CheckNativePortOpen(output, newIp ?? (TestClusterManager.IpPrefix + nodeIdToStart));
            }
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
            var originalStart = start;
            if (_executor is WslCcmProcessExecuter)
            {
                start = false;
            }

            var output = _ccm.BootstrapNode(nodeIdToStart, dataCenterName, start);
            if (originalStart && _executor is WslCcmProcessExecuter)
            {
                _ccm.CheckNativePortOpen(output, TestClusterManager.IpPrefix + nodeIdToStart);
            }
        }

        public void UpdateDseConfig(params string[] yamlChanges)
        {
            _ccm.UpdateDseConfig(yamlChanges);
        }

        public void UpdateConfig(params string[] yamlChanges)
        {
            _ccm.UpdateConfig(yamlChanges);
        }

        public void UpdateConfig(int nodeId, params string[] yamlChanges)
        {
            _ccm.UpdateConfig(nodeId, yamlChanges);
        }
    }
}
