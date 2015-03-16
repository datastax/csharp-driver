using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class CToolCluster : TestGlobals, ITestCluster
    {
        public CToolCluster(string name, int dc1NodeCount, string defaultKeyspace, bool isUsingDefaultConfig = true)
        {
            Name = name;
            Dc1NodeCount = dc1NodeCount;
            DefaultKeyspace = defaultKeyspace;
            IsUsingDefaultConfig = isUsingDefaultConfig;
            IsCreated = false;
            IsBeingCreated = false;
            IsStarted = false;
            IsStarting = false;
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
        public bool IsStarting { get; set; }
        public bool IsRemoved { get; set; }
        public List<string> ExpectedInitialHosts { get; set; }

        public void StartClusterAndClient()
        {
            IsBeingCreated = true;
            // here's where you would create a new C* cluster using CToolBridge
            // here's where you would start the C* cluster using CToolBridge
            InitClient();
            IsBeingCreated = false;
            IsCreated = true;
        }

        public void InitClient()
        {
            Session = new Builder().AddContactPoint(InitialContactPoint).Build().Connect();
            Session.CreateKeyspaceIfNotExists(DefaultKeyspace);
            Session.ChangeKeyspace(DefaultKeyspace);
        }

        public void BootstrapNode(int nodeIdToStart)
        {
            throw new NotImplementedException();
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName)
        {
            throw new NotImplementedException();
        }

        public void DecommissionNode(int nodeId)
        {
            throw new NotImplementedException();
        }

        public void SwitchToThisCluster()
        {
            throw new NotImplementedException();
        }

        public void UseVNodes(string nodesToPopulate)
        {
            throw new NotImplementedException();
        }

        public void ShutDown()
        {
            if (Cluster != null)
                Cluster.Shutdown();
            // TODO: CToolBridge.Stop();
        }

        public void Remove()
        {
            throw new NotImplementedException();
        }

        public void SwitchToThisAndStart()
        {
            throw new NotImplementedException();
        }

        public void Create(bool startCluster = true)
        {
            throw new NotImplementedException();
        }

        public void StopForce(int nodeIdToStop)
        {
            throw new NotImplementedException();
        }

        public void Stop(int nodeIdToStop)
        {
            throw new NotImplementedException();
        }

        public void Start(int nodeIdToStart)
        {
            throw new NotImplementedException();
        }
    }
}
