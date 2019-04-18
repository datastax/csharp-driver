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

using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqExecutionProfileTests : MappingTestBase
    {
        [Test]
        [TestCase(true, true)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(false, false)]
        public async Task Should_ExecuteBatchCorrectlyWithExecutionProfile_When_ExecutionProfileIsProvided(bool batchV1, bool async)
        {
            IStatement statement = null;
            var session = batchV1
                ? GetSession<SimpleStatement>(new RowSet(), stmt => statement = stmt, ProtocolVersion.V1)
                : GetSession<BatchStatement>(new RowSet(), stmt => statement = stmt, ProtocolVersion.V2);

            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.StringValue, cm => cm.WithName("val"))
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");

            var batch = session.CreateBatch();
            Assert.IsTrue(batchV1 ? batch.GetType() == typeof(BatchV1) : batch.GetType() == typeof(BatchV2));

            const int updateCount = 3;
            var table = GetTable<AllTypesEntity>(session, map);
            var updateGuids = Enumerable.Range(0, updateCount).Select(_ => Guid.NewGuid()).ToList();
            var updateCqls = updateGuids.Select(guid => table
                .Where(_ => _.UuidValue == guid)
                .Select(_ => new AllTypesEntity { StringValue = "newStringFor" + guid })
                .Update());
            batch.Append(updateCqls);

            if (async)
            {
                await batch.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                batch.Execute("testProfile");
            }

            Assert.NotNull(statement);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);

            if (!batchV1)
            {
                var batchStatement = (BatchStatement) statement;
                foreach (var updateGuid in updateGuids)
                {
                    var updateStatement = batchStatement.Queries.First(_ => _.QueryValues.Length == 2 && _.QueryValues[1] as Guid? == updateGuid) as SimpleStatement;
                    Assert.IsNotNull(updateStatement);
                    Assert.IsNotNull(updateStatement.QueryValues);
                    Assert.AreEqual(2, updateStatement.QueryValues.Length);
                    Assert.AreEqual("newStringFor" + updateGuid, updateStatement.QueryValues[0]);
                    Assert.AreEqual("UPDATE tbl1 SET val = ? WHERE id = ?", updateStatement.QueryString);
                }
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteCqlCommandWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            var session = GetSession((q, v) => query = q);
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            var cqlDelete = table.Where(t => t.IntValue == 100).Delete();

            if (async)
            {
                await cqlDelete.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                cqlDelete.Execute("testProfile");
            }

            Assert.AreEqual("DELETE FROM ks1.tbl1 WHERE id = ?", query);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteCqlConditionalCommandWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            var session = GetSession((q, v) => query = q);
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            var cqlUpdateIf = table.Where(t => t.IntValue == 100).Select(t => new AllTypesEntity { DoubleValue = 123D }).UpdateIf(t => t.DoubleValue > 123D);

            if (async)
            {
                await cqlUpdateIf.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                cqlUpdateIf.Execute("testProfile");
            }

            Assert.AreEqual("UPDATE ks1.tbl1 SET val = ? WHERE id = ? IF val > ?", query);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteCqlQueryWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            var session = GetSession((q, v) => query = q);
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            var cqlSelect = table.Where(t => t.IntValue == 100).Select(t => new AllTypesEntity { DoubleValue = t.DoubleValue });

            if (async)
            {
                await cqlSelect.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                cqlSelect.Execute("testProfile");
            }

            Assert.AreEqual("SELECT val FROM ks1.tbl1 WHERE id = ?", query);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecutePagedCqlQueryWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            var session = GetSession((q, v) => query = q);
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            var cqlSelect = table.Where(t => t.IntValue == 100).Select(t => new AllTypesEntity { DoubleValue = t.DoubleValue });

            if (async)
            {
                await cqlSelect.ExecutePagedAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                cqlSelect.ExecutePaged("testProfile");
            }

            Assert.AreEqual("SELECT val FROM ks1.tbl1 WHERE id = ?", query);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteCqlQuerySingleElementWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            var session = GetSession((q, v) => query = q);
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            var cqlFirst = table.Where(t => t.IntValue == 100).Select(t => new AllTypesEntity { DoubleValue = t.DoubleValue }).First();
            
            if (async)
            {
                await cqlFirst.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                cqlFirst.Execute("testProfile");
            }

            Assert.AreEqual("SELECT val FROM ks1.tbl1 WHERE id = ? LIMIT ?", query);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_ExecuteCqlScalarQueryWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            var session = GetSession((q, v) => query = q);
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            var cqlCount = table.Where(t => t.IntValue == 100).Select(t => new AllTypesEntity { DoubleValue = t.DoubleValue }).Count();
            
            if (async)
            {
                await cqlCount.ExecuteAsync("testProfile").ConfigureAwait(false);
            }
            else
            {
                cqlCount.Execute("testProfile");
            }

            Assert.AreEqual("SELECT count(*) FROM ks1.tbl1 WHERE id = ?", query);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
    }
}