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
        public CcmBridge CcmBridge { get; private set; }
        private readonly string _version;

        public CcmCluster(string version, string name, int dc1NodeCount, string clusterIpPrefix, string defaultKeyspace, bool isUsingDefaultConfig = true) :
            this(version, name, dc1NodeCount, 0, clusterIpPrefix, defaultKeyspace, isUsingDefaultConfig)
        {
        }

        public CcmCluster(string version, string name, int dc1NodeCount, int dc2NodeCount, string clusterIpPrefix, string defaultKeyspace, bool isUsingDefaultConfig = true)
        {
            _version = version;
            Name = name;
            Dc1NodeCount = dc1NodeCount;
            Dc2NodeCount = dc2NodeCount;
            DefaultKeyspace = defaultKeyspace;
            IsUsingDefaultConfig = isUsingDefaultConfig;
            IsCreated = false;
            IsBeingCreated = false;
            IsStarted = false;
            ClusterIpPrefix = clusterIpPrefix;
            InitialContactPoint = ClusterIpPrefix + "1";
            SetExpectedHosts();
        }

        private void SetExpectedHosts()
        {
            if (ExpectedInitialHosts == null)
                ExpectedInitialHosts = new List<string>();

            // number of hosts should equal the total number of nodes in both data centers
            for (int i = 1; i <= Dc1NodeCount + Dc2NodeCount; i++)
                ExpectedInitialHosts.Add(ClusterIpPrefix + i);
        }

        public string Name { get; set; }
        public Builder Builder { get; set; }
        public Cluster Cluster { get; set; }
        public ISession Session { get; set; }
        public int Dc1NodeCount { get; set; }
        public int Dc2NodeCount { get; set; }
        public string InitialContactPoint { get; set; }
        public string ClusterIpPrefix { get; set; }
        public string DefaultKeyspace { get; set; }
        public bool IsBeingCreated { get; set; }
        public bool IsCreated { get; set; }
        public bool IsStarted { get; set; }
        public bool IsUsingDefaultConfig { get; set; }
        public bool IsRemoved { get; set; }
        public List<string> ExpectedInitialHosts { get; set; }

        // So far, for CCM only
        private ProcessOutput _proc { get; set; }

        public void Create(bool startTheCluster = true, string[] jvmArgs = null)
        {
            // if it's already being created in another thread, then wait until this step is complete
            if (!IsBeingCreated)
            {
                IsBeingCreated = true;
                if (Dc2NodeCount > 0)
                    CcmBridge = CcmBridge.Create(Name, ClusterIpPrefix, Dc1NodeCount, Dc2NodeCount, _version, startTheCluster);
                else
                    CcmBridge = CcmBridge.Create(Name, ClusterIpPrefix, Dc1NodeCount, _version, startTheCluster, jvmArgs);
                IsBeingCreated = false;
                IsCreated = true;
                if (startTheCluster)
                    IsStarted = true;
            }
            int sleepMs = 300;
            int sleepMsMax = 60000;
            int totalMsSlept = 0;
            while (IsBeingCreated || !IsCreated)
            {
                Trace.TraceInformation(string.Format("Cluster with name: {0}, CcmDir: {1} is being created. Sleeping another {2} MS ... ", Name,
                    CcmBridge.CcmDir.FullName, sleepMs));
                Thread.Sleep(sleepMs);
                totalMsSlept += sleepMs;
                if (totalMsSlept > sleepMsMax)
                {
                    throw new Exception("Failed to create cluster in " + sleepMsMax + " MS!");
                }
            }
        }

        public void InitClient()
        {
            if (Cluster != null && IsStarted)
                Cluster.Shutdown();
            if (Builder == null)
                Builder = new Builder();
            Cluster = Builder.AddContactPoint(InitialContactPoint).Build();
            Session = Cluster.Connect();
            Session.CreateKeyspaceIfNotExists(DefaultKeyspace);
            TestUtils.WaitForSchemaAgreement(Cluster);
            Session.ChangeKeyspace(DefaultKeyspace);
        }

        public void ShutDown()
        {
            if (!IsStarted)
                return;

            if (Cluster != null)
                Cluster.Shutdown();
            CcmBridge.Stop();
            IsStarted = false;
        }

        public void Remove()
        {
            Trace.TraceInformation(string.Format("Removing Cluster with Name: '{0}', InitialContactPoint: {1}, and CcmDir: {2}", Name, InitialContactPoint, CcmBridge.CcmDir));
            CcmBridge.SwitchToThis();
            CcmBridge.Remove();
            IsRemoved = true;
        }

        public void DecommissionNode(int nodeId)
        {
            CcmBridge.DecommissionNode(nodeId);
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
            CcmBridge.SwitchToThis();
        }

        public void UseVNodes(string nodesToPopulate)
        {
            CcmBridge.ExecuteCcm("remove");
            CcmBridge.ExecuteCcm(String.Format("create {0} -v {1}", CcmBridge.Name, _version));
            CcmBridge.ExecuteCcm(String.Format("populate -n {0} -i {1} --vnodes", nodesToPopulate, CcmBridge.IpPrefix), CcmBridge.DefaultCmdTimeout, true);
            CcmBridge.ExecuteCcm("start", CcmBridge.DefaultCmdTimeout, true);
        }

        public void SwitchToThisAndStart()
        {
            // only send the 'start' command if it isn't already in the process of starting
            if (!IsStarted)
            {
                SwitchToThisCluster();
                Start();
            }
            IsStarted = true;
        }

        public void StopForce(int nodeIdToStop)
        {
            CcmBridge.StopForce(nodeIdToStop);
        }

        public void Stop(int nodeIdToStop)
        {
            CcmBridge.Stop(nodeIdToStop);
        }

        public void Start()
        {
            CcmBridge.Start();
            IsStarted = true;
        }

        public void Start(int nodeIdToStart, string additionalArgs = null)
        {
            CcmBridge.Start(nodeIdToStart, additionalArgs);
        }

        public void BootstrapNode(int nodeIdToStart)
        {
            CcmBridge.BootstrapNode(nodeIdToStart);
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName)
        {
            CcmBridge.BootstrapNode(nodeIdToStart, dataCenterName);
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
