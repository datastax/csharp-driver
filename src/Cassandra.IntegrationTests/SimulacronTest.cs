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
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.SystemTables;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.SessionManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [TestFixture, Category(TestCategory.Short)]
    public abstract class SimulacronTest : TestGlobals
    {
        private readonly bool _shared;
        private readonly SimulacronOptions _options;
        private readonly bool _connect;
        private readonly string _keyspace;

        public SimulacronTest() : this(false, null, true, null)
        {
        }

        public SimulacronTest(bool shared = false, SimulacronOptions options = null, bool connect = true, string keyspace = null)
        {
            _shared = shared;
            _options = options ?? new SimulacronOptions();
            _connect = connect;
            _keyspace = keyspace;
        }
        
        protected ISession Session { get; private set; }

        internal IInternalSession InternalSession => (IInternalSession) Session;

        protected ICluster SessionCluster => Session?.Cluster;

        protected SimulacronCluster TestCluster { get; private set; }

        protected IEnumerable<List<object>> GetBoundStatementExecutionParameters(string cql)
        {
            var queries = TestCluster.GetQueries(cql, QueryType.Execute);
            return queries.Select(q => q.Frame.GetQueryMessage().Options.PositionalValues);
        }

        protected string SerializeParameter(object parameter)
        {
            var serializer = Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            return Convert.ToBase64String(serializer.Serialize(parameter));
        }
        
        protected void VerifyBoundStatement(string cql, int count, params object[] positionalParameters)
        {
            VerifyStatement(QueryType.Execute, cql, count, positionalParameters);
        }

        protected void VerifyQuery(string cql, int count, params object[] positionalParameters)
        {
            VerifyStatement(QueryType.Query, cql, count, positionalParameters);
        }

        protected void VerifyStatement(QueryType type, string cql, int count, params object[] positionalParameters)
        {
            var queries = TestCluster.GetQueries(null, type);
            VerifyStatement(queries, cql, count, positionalParameters);
        }

        protected void VerifyStatement(IList<RequestLog> logs, string cql, int count, params object[] positionalParameters)
        {
            var serializer = Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            var paramBytes = positionalParameters.Select(obj => obj == null ? null : Convert.ToBase64String(serializer.Serialize(obj))).ToList();
            var filteredQueries = logs.Where(q => (q.Query == cql || cql == null) && q.Frame.GetQueryMessage().Options.PositionalValues.SequenceEqual(paramBytes));

            Assert.AreEqual(count, filteredQueries.Count());
        }

        protected void VerifyBatchStatement(int count, byte[][] ids, params object[][] parameters)
        {
            VerifyBatchStatement(count, ids.Select(Convert.ToBase64String).ToArray(), parameters);
        }

        protected void VerifyBatchStatement(int count, string[] queries, params object[][] parameters)
        {
            var serializer = Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            var logs = TestCluster.GetQueries(null, QueryType.Batch);

            var paramBytes = parameters.SelectMany(obj => obj.Select(o => o == null ? null : Convert.ToBase64String(serializer.Serialize(o)))).ToArray();
            var filteredQueries = logs.Where(q =>
                q.Frame.GetBatchMessage().Values.SelectMany(l => l).SequenceEqual(paramBytes)
                && q.Frame.GetBatchMessage().QueriesOrIds.SequenceEqual(queries));
            Assert.AreEqual(count, filteredQueries.Count());
        }

        protected void VerifyBatchStatement(int count, string[] queries, Func<BatchMessage, bool> func, params object[][] parameters)
        {
            var serializer = Session.Cluster.Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            var logs = TestCluster.GetQueries(null, QueryType.Batch);

            var paramBytes = parameters.SelectMany(obj => obj.Select(o => o == null ? null : Convert.ToBase64String(serializer.Serialize(o)))).ToArray();
            var filteredQueries = logs.Where(q => 
                q.Frame.GetBatchMessage().Values.SelectMany(l => l).SequenceEqual(paramBytes)
                && q.Frame.GetBatchMessage().QueriesOrIds.SequenceEqual(queries) 
                && func(q.Frame.GetBatchMessage()));
            Assert.AreEqual(count, filteredQueries.Count());
        }

        protected void PrimeSystemSchemaUdt(string keyspace, string type, IEnumerable<StubTableColumn> columns)
        {

        }

        protected void PrimeSystemSchemaTables(string keyspace, string table, IEnumerable<StubTableColumn> columns)
        {
            var version30 = new Version(3, 0);
            var cassandraVersion = TestClusterManager.CassandraVersion;

            if (cassandraVersion >= version30)
            {
                TestCluster.PrimeSystemSchemaTablesV2(keyspace, table, columns);
                return;
            }

            if (cassandraVersion < version30)
            {
                TestCluster.PrimeSystemSchemaTablesV1(keyspace, table, columns);
                return;
            }

            throw new NotSupportedException("Unrecognized cassandra version: " + cassandraVersion);
        }

        [OneTimeSetUp]
        public virtual void OneTimeSetUp()
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
        public virtual void SetUp()
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
                if (_keyspace != null)
                {
                    Session.ChangeKeyspace(_keyspace);
                }
            }
        }

        private void Dispose()
        {
            Session?.Cluster?.Dispose();
            TestCluster?.Dispose();
        }

        protected void SetupNewTestCluster()
        {
            Dispose();
            Session = null;
            TestCluster = null;
            Init();
        }
        
        protected void SetupNewSession(Func<Builder, Builder> builderConfig)
        {
            var session = builderConfig(ClusterBuilder()).AddContactPoint(TestCluster.InitialContactPoint).Build().Connect();
            Session?.Cluster?.Dispose();
            Session = session;
        }

        protected virtual ISession CreateSession()
        {
            return ConfigBuilder(ClusterBuilder()).AddContactPoint(TestCluster.InitialContactPoint).Build().Connect();
        }

        protected virtual Builder ConfigBuilder(Builder b)
        {
            return b;
        }
    }
}