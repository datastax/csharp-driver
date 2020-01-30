//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

using Dse.Test.Integration.TestClusterManagement;

using NUnit.Framework;

namespace Dse.Test.Integration
{
    /// <summary>
    /// Represents a test fixture that on setup, it creates a test cluster available for all tests.
    /// <para>
    /// With a shared session and cluster.
    /// </para>
    /// </summary>
    public abstract class SharedClusterTest : TestGlobals
    {
        private static ITestCluster _reusableInstance;
        private readonly bool _reuse;
        protected readonly List<ICluster> ClusterInstances = new List<ICluster>();

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
        protected Cluster Cluster { get; set; }

        /// <summary>
        /// The shared Session instance of the fixture
        /// </summary>
        protected ISession Session { get; set; }

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

        protected virtual ITestCluster CreateNew(int nodeLength, TestClusterOptions options, bool startCluster)
        {
            return TestClusterManager.CreateNew(nodeLength, options, startCluster);
        }

        protected virtual bool IsSimilarCluster(ITestCluster reusableInstance, TestClusterOptions options, int nodeLength)
        {
            return ReferenceEquals(reusableInstance, TestClusterManager.LastInstance)
                   && ((options != null && options.Equals(TestClusterManager.LastOptions)) ||
                       (options == null && TestClusterManager.LastOptions == null))
                   && nodeLength == TestClusterManager.LastAmountOfNodes;
        }

        [OneTimeSetUp]
        public virtual void OneTimeSetUp()
        {
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;

            if (_reuse && _reusableInstance != null && IsSimilarCluster(SharedClusterTest._reusableInstance, Options, AmountOfNodes))
            {
                Trace.WriteLine("Reusing ccm instance");
                TestCluster = _reusableInstance;
            }
            else
            {
                TestCluster = CreateNew(AmountOfNodes, Options, true);
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
                CreateCommonSession();
                if (SetupQueries != null)
                {
                    ExecuteSetupQueries();
                }
            }
        }
        
        protected virtual void CreateCommonSession()
        {
            Cluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint)
                             .WithQueryTimeout(60000)
                             .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000).SetReadTimeoutMillis(22000))
                             .Build();
            Session = (Session)Cluster.Connect();
            Session.CreateKeyspace(KeyspaceName, null, false);
            Session.ChangeKeyspace(KeyspaceName);
        }

        protected virtual void ExecuteSetupQueries()
        {
            foreach (var query in SetupQueries)
            {
                Session.Execute(query);
            }
        }

        [OneTimeTearDown]
        public virtual void OneTimeTearDown()
        {
            if (Cluster != null)
            {
                Cluster.Shutdown(TestClusterManager.Executor.GetDefaultTimeout());
            }
            //Shutdown the other instances created by helper methods
            foreach (var c in ClusterInstances)
            {
                c.Shutdown(TestClusterManager.Executor.GetDefaultTimeout());
            }
            ClusterInstances.Clear();
        }
        
        protected ISession GetNewTemporarySession(string keyspace = null)
        {
            return GetNewTemporaryCluster().Connect(keyspace);
        }
        
        [TearDown]
        public virtual void TearDown()
        {
            foreach (var c in ClusterInstances)
            {
                try
                {
                    c.Dispose();
                }
                catch
                {
                    // ignored
                }
            }
            ClusterInstances.Clear();
        }

        protected virtual ICluster GetNewTemporaryCluster(Action<Builder> build = null)
        {
            var builder = 
                Cluster.Builder()
                       .AddContactPoint(TestCluster.InitialContactPoint)
                       .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000).SetReadTimeoutMillis(22000));
            build?.Invoke(builder);
            var cluster = builder.Build();
            ClusterInstances.Add(cluster);
            return cluster;
        }

        protected void SetBaseSession(ISession session)
        {
            Session = session;
        }
    }
}