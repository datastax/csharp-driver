//
//       Copyright (C) 2019 DataStax Inc.
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
//

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.ExecutionProfiles
{
    [TestFixture]
    [Category("short")]
    public class LinqExecutionProfileTests
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
            _session = Cluster.Builder()
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
            _simulacronCluster.Remove().Wait();
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteDeleteWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToDelete = _entityList[0];
            var cql = $"DELETE FROM {_keyspace}.all_data WHERE StringType = ?";
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            var deleteQuery = _table.Where(m => m.StringType == entityToDelete.StringType).Delete();

            if (async)
            {
                await deleteQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                deleteQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteDeleteIfWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToDelete = _entityList[0];
            var cql = $"DELETE FROM {_keyspace}.all_data WHERE StringType = ? IF StringType = ?";
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            var deleteQuery = _table.Where(m => m.StringType == entityToDelete.StringType).DeleteIf(m => m.StringType == "test");

            if (async)
            {
                await deleteQuery.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                deleteQuery.Execute("testProfile");
            }

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToUpdate = _entityList[1];
            var cql = $"UPDATE {_keyspace}.all_data SET IntType = ? WHERE StringType = ?";
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
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

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateIfWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToUpdate = _entityList[1];
            var cql = $"UPDATE {_keyspace}.all_data SET IntType = ? WHERE StringType = ? IF IntType = ?";
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
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

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteUpdateIfExistsWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var entityToUpdate = _entityList[1];
            var cql = $"UPDATE {_keyspace}.all_data SET IntType = ? WHERE StringType = ? IF EXISTS";
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
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

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
        }
        
        private object CreatePrimeObject(AllDataTypesEntity allData)
        {
            return new
            {
                StringType = allData.StringType,
                GuidType = allData.GuidType,
                IntType = allData.IntType
            };
        }

        private object CreateThenForPrimeSelect(IEnumerable<AllDataTypesEntity> allData)
        {
            return new
            {
                result = "success",
                delay_in_ms = 0,
                rows = allData.Select(CreatePrimeObject).ToArray(),
                column_types = new
                {
                    StringType = "ascii",
                    GuidType = "uuid",
                    IntType = "int"
                },
                ignore_on_prepare = true
            };
        }

        private void PrimeSelect(IEnumerable<AllDataTypesEntity> allData, string consistencyLevel, string query)
        {
            var primeQuery = new
            {
                when = new
                {
                    query = query,
                    consistency_level = new[] { consistencyLevel }
                },
                then = CreateThenForPrimeSelect(allData)
            };
            _simulacronCluster.Prime(primeQuery);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteFetchWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var cql = $"SELECT IntType FROM {_keyspace}.all_data";
            PrimeSelect(_entityList, "TWO", cql);
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            var selectQuery = _table
                              .Select(m => new AllDataTypesEntity { IntType = m.IntType });
            
            var result = async 
                ? (await selectQuery.ExecuteAsync("testProfile").ConfigureAwait(false)).ToList() 
                : selectQuery.Execute("testProfile").ToList();

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
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
            PrimeSelect(_entityList, "TWO", cql);
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            var selectQuery = _table.Select(m => new AllDataTypesEntity { IntType = m.IntType });

            var result = async 
                ? (await selectQuery.ExecutePagedAsync("testProfile").ConfigureAwait(false)).ToList() 
                : selectQuery.ExecutePaged("testProfile").ToList();

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
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
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
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

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteFirstElementWithProvidedExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var cql = $"SELECT IntType FROM {_keyspace}.all_data LIMIT ?";
            PrimeSelect(_entityList, "TWO", cql);
            var queries = _simulacronCluster.GetQueries(cql, "EXECUTE");
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

            var newQueries = _simulacronCluster.GetQueries(cql, "EXECUTE");
            Assert.AreEqual(queries.Count + 1, newQueries.Count);
            Assert.IsTrue(newQueries.All(q => q.consistency_level == "TWO"));
            Assert.AreEqual(_entityList.First().StringType, result.StringType);
            Assert.AreEqual(_entityList.First().GuidType, result.GuidType);
            Assert.AreEqual(_entityList.First().IntType, result.IntType);
        }
    }
}