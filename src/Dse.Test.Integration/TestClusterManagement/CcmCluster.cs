//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Diagnostics;

namespace Dse.Test.Integration.TestClusterManagement
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
            _ccm.UpdateDseConfig(options.DseYaml);
            _ccm.SetWorkloads(nodeLength, options.Workloads);
        }

        public void InitClient()
        {
            if (Cluster != null)
            {
                Cluster.Shutdown();   
            }
            if (Builder == null)
            {
                Builder = new DseClusterBuilder();   
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
            if (Cluster != null)
            {
                Cluster.Shutdown();   
            }
            _ccm.Stop();
        }

        public void Remove()
        {
            Trace.TraceInformation("Removing Cluster with Name: '{0}', InitialContactPoint: {1}, and CcmDir: {2}", Name, InitialContactPoint, _ccm.CcmDir);
            _ccm.Remove();
        }

        public void Remove(int nodeId)
        {
            Trace.TraceInformation("Removing node '{0}' from cluster '{1}'", nodeId, Name);
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
            _ccm.ExecuteCcm(string.Format("node{0} pause", nodeId));
        }

        public void ResumeNode(int nodeId)
        {
            _ccm.ExecuteCcm(string.Format("node{0} resume", nodeId));
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
            _ccm.SetNodeWorkloads(nodeId, workloads);
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName, bool start = true)
        {
            _ccm.BootstrapNode(nodeIdToStart, dataCenterName, start);
        }

        public void UpdateConfig(params string[] yamlChanges)
        {
            if (yamlChanges == null) return;
            foreach (var setting in yamlChanges)
            {
                _ccm.ExecuteCcm("updateconf \"" + setting + "\"");
            }
        }
    }
}
