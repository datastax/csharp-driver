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
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    public abstract class SharedSimulacronTests : TestGlobals
    {
        private readonly List<Cluster> _clusterInstances = new List<Cluster>();
        private readonly List<SimulacronCluster> _simulacronClusters = new List<SimulacronCluster>();

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
        protected SimulacronCluster TestCluster { get; private set; }

        /// <summary>
        /// The shared cluster instance of the fixture
        /// </summary>
        protected Cluster Cluster { get; set; }

        /// <summary>
        /// The shared Session instance of the fixture
        /// </summary>
        protected Session Session { get; private set; }

        /// <summary>
        /// Gets or sets the name of the default keyspace used for this instance
        /// </summary>
        protected string KeyspaceName { get; set; }

        protected SimulacronOptions Options { get; set; }

        public SharedSimulacronTests() : this(1, true)
        {
        }

        protected SharedSimulacronTests(int amountOfNodes, bool createSession)
        {
            AmountOfNodes = amountOfNodes;
            Options = new SimulacronOptions { Nodes = amountOfNodes.ToString() };
            KeyspaceName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            CreateSession = createSession;
        }

        protected SharedSimulacronTests(SimulacronOptions options, bool createSession = true)
        {
            AmountOfNodes = int.Parse(options.Nodes);
            Options = options;
            KeyspaceName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            CreateSession = createSession;
        }

        protected virtual SimulacronCluster CreateNew(SimulacronOptions options)
        {
            Options = options;
            var c = SimulacronManager.DefaultInstance.CreateNew(options);
            _simulacronClusters.Add(c);
            return c;
        }

        protected virtual SimulacronCluster CreateNew(int nodeLength)
        {
            Options = new SimulacronOptions { Nodes = nodeLength.ToString() };
            var c = SimulacronManager.DefaultInstance.CreateNew(Options);
            _simulacronClusters.Add(c);
            return c;
        }

        [OneTimeSetUp]
        public virtual void OneTimeSetUp()
        {
            TestCluster = CreateNew(Options);

            if (CreateSession)
            {
                Cluster = ClusterBuilder().AddContactPoint(TestCluster.InitialContactPoint)
                                 .WithQueryTimeout(60000)
                                 .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                                 .Build();
                Session = (Session)Cluster.Connect();
                Session.CreateKeyspace(KeyspaceName, null, false);
                Session.ChangeKeyspace(KeyspaceName);
            }
        }

        [OneTimeTearDown]
        public virtual void OneTimeTearDown()
        {
            Cluster?.Shutdown();
            //Shutdown the other instances created by helper methods
            foreach (var c in _clusterInstances)
            {
                c.Shutdown();
            }

            foreach (var c in _simulacronClusters)
            {
                c.Dispose();
            }
        }

        protected virtual ISession GetNewSession(string keyspace = null)
        {
            return GetNewCluster().Connect(keyspace);
        }

        protected virtual ICluster GetNewCluster(Action<Builder> build = null)
        {
            var builder = ClusterBuilder().AddContactPoint(TestCluster.InitialContactPoint);
            build?.Invoke(builder);
            var cluster = builder.Build();
            _clusterInstances.Add(cluster);
            return cluster;
        }
    }
}