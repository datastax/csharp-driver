using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// Represents a test fixture that on setup, it creates a test cluster available for all tests.
    /// <para>
    /// With a shared session and cluster.
    /// </para>
    /// </summary>
    [TestFixture]
    public abstract class SharedClusterTest : TestGlobals
    {
        private static ITestCluster _reusableInstance;
        private readonly bool _reuse;
        private readonly List<Cluster> _clusterInstances = new List<Cluster>();
        /// <summary>
        /// Gets the amount of nodes in the test cluster
        /// </summary>
        protected int AmountOfNodes { get; private set; }

        /// <summary>
        /// Determines if an ISession needs to be created to share during the lifetime of this instance
        /// </summary>
        protected bool CreateSession { get; set; }

        /// <summary>
        /// Gets the Cassandra cluster that is used for testing
        /// </summary>
        protected ITestCluster TestCluster { get; private set; }

        /// <summary>
        /// The shared cluster instance of the fixture
        /// </summary>
        protected Cluster Cluster { get; private set; }

        /// <summary>
        /// The shared Session instance of the fixture
        /// </summary>
        protected Session Session { get; private set; }

        /// <summary>
        /// It executes the queries provided on test fixture setup.
        /// Ignored when null.
        /// </summary>
        protected virtual string[] SetupQueries
        {
            get { return null; }
        }

        /// <summary>
        /// Gets or sets the name of the default keyspace used for this instance
        /// </summary>
        protected string KeyspaceName { get; set; }

        protected TestClusterOptions Options { get; set; }

        protected SharedClusterTest(int amountOfNodes = 1, bool createSession = true, bool reuse = true, TestClusterOptions options = null)
        {
            //only reuse single node clusters
            _reuse = reuse && amountOfNodes == 1;
            AmountOfNodes = amountOfNodes;
            KeyspaceName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            CreateSession = createSession;
            Options = options;
        }

        [OneTimeSetUp]
        public virtual void OneTimeSetUp()
        {
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;

            if (_reuse && _reusableInstance != null 
                       && ReferenceEquals(_reusableInstance, TestClusterManager.LastInstance)
                       && ((Options != null && Options.Equals(TestClusterManager.LastOptions)) || (Options == null && TestClusterManager.LastOptions == null))
                       && AmountOfNodes == TestClusterManager.LastAmountOfNodes)
            {
                Trace.WriteLine("Reusing ccm instance");
                TestCluster = _reusableInstance;
            }
            else
            {
                TestCluster = TestClusterManager.CreateNew(AmountOfNodes, Options);
                if (_reuse)
                {
                    _reusableInstance = TestCluster;
                }
                else
                {
                    _reusableInstance = null;
                }
            }
            if (CreateSession)
            {
                Cluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint)
                    .WithQueryTimeout(60000)
                    .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                    .Build();
                Session = (Session) Cluster.Connect();
                Session.CreateKeyspace(KeyspaceName, null, false);
                Session.ChangeKeyspace(KeyspaceName);
                if (SetupQueries != null)
                {
                    foreach (var query in SetupQueries)
                    {
                        Session.Execute(query);
                    }
                }
            }
        }

        [OneTimeTearDown]
        public virtual void OneTimeTearDown()
        {
            if (Cluster != null)
            {
                Cluster.Shutdown(1000);   
            }
            //Shutdown the other instances created by helper methods
            foreach (var c in _clusterInstances)
            {
                c.Shutdown(1000);
            }
        }

        protected ISession GetNewSession(string keyspace = null)
        {
            return GetNewCluster().Connect(keyspace);
        }

        protected Cluster GetNewCluster(Action<Builder> build = null)
        {
            var builder = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint);
            build?.Invoke(builder);
            var cluster = builder.Build();
            _clusterInstances.Add(cluster);
            return cluster;
        }
    }
}
