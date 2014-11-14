using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests.Base
{
    public class TestCluster : TestGlobals
    {
        private static readonly Logger Logger = new Logger(typeof(TestCluster));

        // each TestCluster has one cluster
        public string Name;
        public Cluster Cluster;
        //public CCMBridge ccmBridge;
        public CcmClusterInfo CcmClusterInfo = null;
        // public Cassandra.CCMBridge.CCMCluster ccmCluster;
        public int NodeCount;
        public string InitialContactPoint;
        public string DefaultKeyspace;
        public bool IsInitializing;
        public bool IsInitialized;
        public ISession Session;

        public TestCluster(string name, int nodeCount, string initialContactPoint, string defaultKeyspace)
        {
            this.DefaultKeyspace = defaultKeyspace;
            this.IsInitialized = false;
            this.IsInitializing = false;
            this.Name = name;
            this.NodeCount = nodeCount;
            this.InitialContactPoint = initialContactPoint;
        }

        public void InitializeCtool()
        {
            IsInitializing = true;
            // here's where you would create a new C* cluster using ctool
            Initialize();
            this.IsInitializing = false;
            this.IsInitialized = true;
        }

        public void InitializeCcm()
        {
            IsInitializing = true;
            CcmClusterInfo = TestUtils.CcmSetup(NodeCount, null, TestKeyspaceDefault);
            Initialize();
            this.IsInitializing = false;
            this.IsInitialized = true;
        }

        public void Initialize()
        {
            Session = new Builder().AddContactPoint(InitialContactPoint).Build().Connect();
            Session.CreateKeyspaceIfNotExists(DefaultKeyspace);
            Session.ChangeKeyspace(DefaultKeyspace);
        }

        public void TearDown()
        {
            if (CcmClusterInfo != null)
            {
                string ipList = string.Join(",", CcmClusterInfo.Cluster.AllHosts().Select(x => x.Address));
                Logger.Info(string.Format("removing cluster -- name: '{0}', hosts: {1}", Name, ipList));
                TestUtils.CcmRemove(CcmClusterInfo);
            }
        }

    }
}
