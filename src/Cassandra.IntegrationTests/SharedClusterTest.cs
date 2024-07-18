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
using System.Collections.Generic;
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
    public abstract class SharedClusterTest : TestGlobals
    {
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

        protected SharedClusterTest(
            int amountOfNodes = 1, bool createSession = true, TestClusterOptions options = null)
        {
            AmountOfNodes = amountOfNodes;
            KeyspaceName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            CreateSession = createSession;
            Options = options;
        }

        protected SharedClusterTest(
            Func<TestClusterOptions> options, int amountOfNodes = 1, bool createSession = true) : this(amountOfNodes, createSession, options?.Invoke())
        {
        }

        protected virtual ITestCluster CreateNew(int nodeLength, TestClusterOptions options, bool startCluster)
        {
            return TestClusterManager.CreateNew(nodeLength, options, startCluster);
        }
        
        [OneTimeSetUp]
        public virtual void OneTimeSetUp()
        {
            TestCluster = CreateNew(AmountOfNodes, Options, true);
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
            var builder = ClusterBuilder().AddContactPoint(TestCluster.InitialContactPoint)
                                          .WithQueryTimeout(60000)
                                          .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000).SetReadTimeoutMillis(22000));
            Cluster = builder.Build();
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
                ClusterBuilder()
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