using System;
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class CcmCluster : TestGlobals, ITestCluster
    {
        private static readonly Logger Logger = new Logger(typeof(CcmCluster));

        public CcmBridge CcmBridge;
        public CcmClusterInfo CcmClusterInfo = null;

        public CcmCluster(string name, int dc1NodeCount, string clusterIpPrefix, string defaultKeyspace, bool isUsingDefaultConfig = true) :
            this(name, dc1NodeCount, 0, clusterIpPrefix, defaultKeyspace, isUsingDefaultConfig)
        {
        }

        public CcmCluster(string name, int dc1NodeCount, int dc2NodeCount, string clusterIpPrefix, string defaultKeyspace, bool isUsingDefaultConfig = true)
        {
            Name = name;
            Dc1NodeCount = dc1NodeCount;
            Dc2NodeCount = dc2NodeCount;
            DefaultKeyspace = defaultKeyspace;
            IsUsingDefaultConfig = isUsingDefaultConfig;
            IsCreated = false;
            IsBeingCreated = false;
            IsStarted = false;
            IsStarting = false;
            ClusterIpPrefix = clusterIpPrefix;
            InitialContactPoint = ClusterIpPrefix + "1";
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
        public bool IsStarting { get; set; }
        public bool IsUsingDefaultConfig { get; set; }
        public bool IsRemoved { get; set; }

        // So far, for CCM only
        private ProcessOutput _proc { get; set; }

        public void Create(bool startTheCluster = true)
        {
            // if it's already being created, then wait until this step is complete
            if (!IsBeingCreated)
            {
                IsBeingCreated = true;
                if (startTheCluster)
                    IsStarting = true;
                if (Dc2NodeCount > 0)
                    CcmBridge = CcmBridge.Create(Name, ClusterIpPrefix, Dc1NodeCount, Dc2NodeCount, CassandraVersion.ToString(), startTheCluster);
                else
                    CcmBridge = CcmBridge.Create(Name, ClusterIpPrefix, Dc1NodeCount, CassandraVersion.ToString(), startTheCluster);
                IsBeingCreated = false;
                IsCreated = true;
            }
            int sleepMs = 300;
            int sleepMsMax = 60000;
            int totalMsSlept = 0;
            while (IsBeingCreated || !IsCreated)
            {
                Logger.Info(string.Format("Cluster with name: {0}, CcmDir: {1} is being created. Sleeping another {2} MS ... ", Name,
                    CcmBridge.CcmDir.FullName, sleepMs));
                Thread.Sleep(sleepMs);
                totalMsSlept += sleepMs;
                if (totalMsSlept > sleepMsMax)
                {
                    throw new Exception("Failed to create cluster in " + sleepMsMax + " MS!");
                }
            }

            if (startTheCluster)
                SwitchToThisStartAndConnect();
        }

        public void CreateAndStart()
        {
            Create(true);
        }

        public void StartClusterAndClient()
        {
            IsStarting = true;
            SwitchToThisStartAndConnect();
        }

        public void InitClient()
        {
            if (Builder == null)
                Builder = new Builder();
            if (Cluster != null)
                Cluster.Shutdown();
            Cluster = Builder.AddContactPoint(InitialContactPoint).Build();
            Session = Cluster.Connect();
            Session.CreateKeyspaceIfNotExists(DefaultKeyspace);
            Session.ChangeKeyspace(DefaultKeyspace);
        }

        public void ShutDown()
        {
            if (Cluster != null)
                Cluster.Shutdown();
            CcmBridge.Stop();
            IsStarted = false;
        }

        public void Remove()
        {
            Logger.Info(string.Format("Removing Cluster with Name: '{0}', InitialContactPoint: {1}, and CcmDir: {2}", Name, InitialContactPoint, CcmBridge.CcmDir));
            CcmBridge.Remove();
            IsRemoved = true;
        }

        public void DecommissionNode(int nodeId)
        {
            CcmBridge.DecommissionNode(nodeId);
        }

        public void SwitchToThisCluster()
        {
            CcmBridge.SwitchToThis();
        }

        public void SwitchToThisStartAndConnect()
        {
            // only send the 'start' command if it isn't already in the process of starting
            if (!IsStarted && !IsStarting)
            {
                SwitchToThisCluster();
                Start();
                IsStarting = true;
            }
            
            // wait for it to finish starting if needs be
            try
            {
                InitClient();
                IsStarting = false;
                IsStarted = true;
                IsBeingCreated = false;
                IsCreated = true;
            }
            catch (Cassandra.NoHostAvailableException e)
            {
                Logger.Error(string.Format("NoHostAvailableException was thrown, cluster with Name: {0}, InitialContactPoint: {1}, CcmDir: {2} was not successfully started!", Name, InitialContactPoint, CcmBridge.CcmDir));
                Logger.Error("Error Message: " + e.Message);
                Logger.Error("Error StackTrace: " + e.StackTrace);
            }
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
        }

        public void Start(int nodeIdToStart)
        {
            CcmBridge.Start(nodeIdToStart);
        }

        public void BootstrapNode(int nodeIdToStart)
        {
            CcmBridge.BootstrapNode(nodeIdToStart);
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName)
        {
            CcmBridge.BootstrapNode(nodeIdToStart, dataCenterName);
        }


    }
}
