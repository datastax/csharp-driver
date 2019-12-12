//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [TestFixture, Category("short")]
    public class SimulacronTest
    {
        private readonly bool _shared;
        private readonly SimulacronOptions _options;
        private readonly bool _connect;

        public SimulacronTest(bool shared = false, SimulacronOptions options = null, bool connect = true)
        {
            _shared = shared;
            _options = options ?? new SimulacronOptions();
            _connect = connect;
        }

        protected ISession Session { get; private set; }

        protected SimulacronCluster TestCluster { get; private set; }

        protected IEnumerable<List<object>> GetBoundStatementExecutionParameters(string cql)
        {
            var queries = TestCluster.GetQueries(cql, "EXECUTE");
            return queries.Select(q => q.Frame.GetQueryMessage().Options.PositionalValues);
        }

        protected string SerializeParameter(object parameter)
        {
            var serializer = Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            return Convert.ToBase64String(serializer.Serialize(parameter));
        }
        
        protected void VerifyBoundStatement(string cql, int count, params object[] positionalParameters)
        {
            var serializer = Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            var queries = TestCluster.GetQueries(cql, "EXECUTE");

            var paramBytes = positionalParameters.Select(obj => Convert.ToBase64String(serializer.Serialize(obj))).ToList();
            var filteredQueries = queries.Where(q => q.Frame.GetQueryMessage().Options.PositionalValues.SequenceEqual(paramBytes));
            Assert.AreEqual(count, filteredQueries.Count());
        }

        protected void VerifyBatchStatement(int count, string[] queries, object[][] parameters)
        {
            var serializer = Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            var logs = TestCluster.GetQueries(null, "BATCH");

            var paramBytes = parameters.SelectMany(obj => obj.Select(o => Convert.ToBase64String(serializer.Serialize(o)))).ToArray();
            var filteredQueries = logs.Where(q => 
                q.Frame.GetBatchMessage().Values.SelectMany(l => l).SequenceEqual(paramBytes)
                && q.Frame.GetBatchMessage().QueriesOrIds.SequenceEqual(queries));
            Assert.AreEqual(count, filteredQueries.Count());
        }

        [OneTimeSetUp]
        public virtual void OneTimeSetup()
        {
            if (_shared)
            {
                Init();
            }
        }

        [OneTimeTearDown]
        public virtual void OneTimeTearDown()
        {
            if (_shared)
            {
                Dispose();
            }
        }

        [SetUp]
        public virtual void SetupTest()
        {
            if (!_shared)
            {
                Init();
            }
        }

        [TearDown]
        public virtual void TearDown()
        {
            if (!_shared)
            {
                Dispose();
            }
        }

        private void Init()
        {
            TestCluster = SimulacronCluster.CreateNew(_options);
            if (_connect)
            {
                Session = CreateSession();
            }
        }

        private void Dispose()
        {
            Session?.Cluster?.Dispose();
            TestCluster?.Dispose();
        }

        private ISession CreateSession()
        {
            return Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build().Connect();
        }
    }
}