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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.ExecutionProfiles
{
    [TestFixture]
    [Category(TestCategory.Short)]
    public class LinqExecutionProfileTests : TestGlobals
    {
        private ISession _session;
        private string _keyspace;
        private Table<AllDataTypesEntity> _table;
        private List<AllDataTypesEntity> _entityList;
        private SimulacronCluster _simulacronCluster;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

            _simulacronCluster = SimulacronCluster.CreateNew(3);
            _session = ClusterBuilder()
                              .AddContactPoint(_simulacronCluster.InitialContactPoint)
                              .WithExecutionProfiles(opts => opts
                                                             .WithProfile("testProfile", profile => profile
                                                                 .WithConsistencyLevel(ConsistencyLevel.Two))
                                                             .WithDerivedProfile("testDerivedProfile", "testProfile", profile => profile
                                                                 .WithConsistencyLevel(ConsistencyLevel.One)))
                              .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.Any))
                              .Build().Connect(_keyspace);

            _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();
            var mapConfig = new Map<AllDataTypesEntity>()
                            .PartitionKey(s => s.StringType)
                            .ClusteringKey(s => s.GuidType)
                            .TableName("all_data")
                            .KeyspaceName(_keyspace);
            _table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration().Define(mapConfig));
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _session.Cluster.Dispose();
            _simulacronCluster.RemoveAsync().Wait();
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteDeleteWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToDelete = _entityList[0];
            var cql = $"DELETE FROM {_keyspace}.all_data WHERE StringType = ?";
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var deleteQuery = _table.Where(m => m.StringType == entityToDelete.StringType).Delete();

            if (async)
            {
                await deleteQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                deleteQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteDeleteIfWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToDelete = _entityList[0];
            var cql = $"DELETE FROM {_keyspace}.all_data WHERE StringType = ? IF StringType = ?";
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var deleteQuery = _table.Where(m => m.StringType == entityToDelete.StringType).DeleteIf(m => m.StringType == "test");

            if (async)
            {
                await deleteQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                deleteQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToUpdate = _entityList[1];
            var cql = $"UPDATE {_keyspace}.all_data SET IntType = ? WHERE StringType = ?";
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var updateQuery = _table
                              .Where(m => m.StringType == entityToUpdate.StringType)
                              .Select(m => new AllDataTypesEntity { IntType = 5 })
                              .Update();

            if (async)
            {
                await updateQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                updateQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateIfWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToUpdate = _entityList[1];
            var cql = $"UPDATE {_keyspace}.all_data SET IntType = ? WHERE StringType = ? IF IntType = ?";
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var updateQuery = _table
                              .Where(m => m.StringType == entityToUpdate.StringType)
                              .Select(m => new AllDataTypesEntity { IntType = 5 })
                              .UpdateIf(m => m.IntType == 4);

            if (async)
            {
                await updateQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                updateQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateIfExistsWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToUpdate = _entityList[1];
            var cql = $"UPDATE {_keyspace}.all_data SET IntType = ? WHERE StringType = ? IF EXISTS";
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var updateQuery = _table
                              .Where(m => m.StringType == entityToUpdate.StringType)
                              .Select(m => new AllDataTypesEntity { IntType = 5 })
                              .UpdateIfExists();

            if (async)
            {
                await updateQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                updateQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }
        
        private object[] CreatePrimeObject(AllDataTypesEntity allData)
        {
            return new object [] { allData.StringType, allData.GuidType, allData.IntType };
        }

        private IThenFluent CreateThenForPrimeSelect(IWhenFluent when, IEnumerable<AllDataTypesEntity> allData)
        {
            return when.ThenRowsSuccess(
                           new[] { ("StringType", DataType.Ascii), ("GuidType", DataType.Uuid), ("IntType", DataType.Int) },
                           rows => rows.WithRows(allData.Select(CreatePrimeObject).ToArray()))
                       .WithIgnoreOnPrepare(true);
        }

        private void PrimeSelect(IEnumerable<AllDataTypesEntity> allData, ConsistencyLevel consistencyLevel, string query)
        {
            var primeQuery = 
                CreateThenForPrimeSelect(
                        SimulacronBase.PrimeBuilder().WhenQuery(query, when => when.WithConsistency(consistencyLevel)), 
                        allData)
                    .BuildRequest();
            _simulacronCluster.Prime(primeQuery);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteFetchWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var cql = $"SELECT IntType FROM {_keyspace}.all_data";
            PrimeSelect(_entityList, ConsistencyLevel.Two, cql);
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var selectQuery = _table
                              .Select(m => new AllDataTypesEntity { IntType = m.IntType });
            
            var result = async 
                ? (await selectQuery.ExecuteAsync("testProfile").ConfigureAwait(false)).ToList() 
                : selectQuery.Execute("testProfile").ToList();

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
            for (var i = 0; i < _entityList.Count; i++)
            {
                Assert.AreEqual(_entityList[i].StringType, result[i].StringType);
                Assert.AreEqual(_entityList[i].GuidType, result[i].GuidType);
                Assert.AreEqual(_entityList[i].IntType, result[i].IntType);
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteFetchPagedWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var cql = $"SELECT IntType FROM {_keyspace}.all_data";
            PrimeSelect(_entityList, ConsistencyLevel.Two, cql);
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var selectQuery = _table.Select(m => new AllDataTypesEntity { IntType = m.IntType });

            var result = async 
                ? (await selectQuery.ExecutePagedAsync("testProfile").ConfigureAwait(false)).ToList() 
                : selectQuery.ExecutePaged("testProfile").ToList();

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
            for (var i = 0; i < _entityList.Count; i++)
            {
                Assert.AreEqual(_entityList[i].StringType, result[i].StringType);
                Assert.AreEqual(_entityList[i].GuidType, result[i].GuidType);
                Assert.AreEqual(_entityList[i].IntType, result[i].IntType);
            }
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteScalarWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToSelect = _entityList[1];
            var cql = $"SELECT count(*) FROM {_keyspace}.all_data WHERE StringType = ?";
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            // ReSharper disable once ReplaceWithSingleCallToCount
            var selectQuery = _table
                              .Where(m => m.StringType == entityToSelect.StringType)
                              .Count();

            if (async)
            {
                await selectQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                selectQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteFirstElementWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var cql = $"SELECT IntType FROM {_keyspace}.all_data LIMIT ?";
            PrimeSelect(_entityList, ConsistencyLevel.Two, cql);
            var queries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            var selectQuery = _table
                              .Select(m => new AllDataTypesEntity { IntType = m.IntType })
                              .First();

            AllDataTypesEntity result;
            if (async)
            {
                result = await selectQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                result = selectQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, QueryType.Execute);
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.ConsistencyLevel == ConsistencyLevel.Two));
            Assert.AreEqual(_entityList.First().StringType, result.StringType);
            Assert.AreEqual(_entityList.First().GuidType, result.GuidType);
            Assert.AreEqual(_entityList.First().IntType, result.IntType);
        }
    }
}