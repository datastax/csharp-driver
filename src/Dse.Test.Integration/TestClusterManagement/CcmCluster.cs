using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class CcmCluster : ITestCluster
    {
        public string Name { get; set; }
        public Builder Builder { get; set; }
        public Cluster Cluster { get; set; }
        public ISession Session { get; set; }
        public string InitialContactPoint { get; set; }
        public string ClusterIpPrefix { get; set; }
        public string DefaultKeyspace { get; set; }
        private readonly string _version;
        private CcmBridge _ccm;

        public CcmCluster(string version, string name, string clusterIpPrefix, string defaultKeyspace)
        {
            _version = version;
            Name = name;
            DefaultKeyspace = defaultKeyspace;
            ClusterIpPrefix = clusterIpPrefix;
            InitialContactPoint = ClusterIpPrefix + "1";
        }

        public void Create(int nodeLength, TestClusterOptions options = null)
        {
            if (options == null)
            {
                options = TestClusterOptions.Default;
            }
            _ccm = new CcmBridge(Name, ClusterIpPrefix);
            _ccm.Create(_version, options.UseSsl);
            _ccm.Populate(nodeLength, options.Dc2NodeLength, options.UseVNodes);
        }

        public void InitClient()
        {
            if (Cluster != null)
            {
                Cluster.Shutdown();   
            }
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

        public void DecommissionNode(int nodeId)
        {
            _ccm.DecommissionNode(nodeId);
        }

        public void PauseNode(int nodeId)
        {
            CcmBridge.ExecuteCcm(string.Format("node{0} pause", nodeId));
        }

        public void ResumeNode(int nodeId)
        {
            CcmBridge.ExecuteCcm(string.Format("node{0} resume", nodeId));
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

        public void BootstrapNode(int nodeIdToStart)
        {
            _ccm.BootstrapNode(nodeIdToStart);
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName)
        {
            _ccm.BootstrapNode(nodeIdToStart, dataCenterName);
        }

        public void UpdateConfig(params string[] yamlChanges)
        {
            if (yamlChanges == null) return;
            foreach (var setting in yamlChanges)
            {
                CcmBridge.ExecuteCcm("updateconf \"" + setting + "\"");
            }
        }
    }
}
