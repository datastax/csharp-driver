using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests.Base
{
    public class TestCluster : TestGlobals
    {
        private static Logger logger = new Logger(typeof(TestCluster));

        // each TestCluster has one cluster
        public string name;
        public Cluster cluster;
        //public CCMBridge ccmBridge;
        public CcmClusterInfo ccmClusterInfo;
        // public Cassandra.CCMBridge.CCMCluster ccmCluster;
        public int nodeCount;
        public string initialContactPoint;
        public string defaultKeyspace;
        public bool isInitializing;
        public bool isInitialized;
        public ISession session;

        public TestCluster(string name, int nodeCount, string initialContactPoint, string defaultKeyspace)
        {
            this.defaultKeyspace = defaultKeyspace;
            this.isInitialized = false;
            this.isInitializing = false;
            this.name = name;
            this.nodeCount = nodeCount;
            this.initialContactPoint = initialContactPoint;
        }

        ~TestCluster()
        {
            try
            {
                TestUtils.CcmRemove(ccmClusterInfo);
            }
            catch (Exception e)
            {
                Console.Out.WriteLine("CCM Removal failed with unexpected error message: " + e.Message);
                Console.Out.WriteLine("Stack Trace: " + e.StackTrace);
            }
        }

        public void initialize_ctool()
        {
            isInitializing = true;
            // here's where you would create a new C* cluster using ctool
            initialize();
            this.isInitializing = false;
            this.isInitialized = true;
        }

        public void initialize_ccm()
        {
            isInitializing = true;
            ccmClusterInfo = TestUtils.CcmSetup(nodeCount, null, TEST_KEYSPACE_DEFAULT);
            initialize();
            this.isInitializing = false;
            this.isInitialized = true;
        }

        public void initialize()
        {
            session = new Builder().AddContactPoint(initialContactPoint).Build().Connect();
            session.CreateKeyspaceIfNotExists(defaultKeyspace);
            session.ChangeKeyspace(defaultKeyspace);
        }

        public void tearDown()
        {
            string ipList = string.Join(",", ccmClusterInfo.Cluster.AllHosts().Select(x => x.Address));
            logger.Info(string.Format("removing cluster -- name: '{0}', hosts: {1}", name, ipList));
            TestUtils.CcmRemove(ccmClusterInfo);
        }

    }
}
