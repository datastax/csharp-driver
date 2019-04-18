using System;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    public class LinqBatchStatementUnitTests : MappingTestBase
    {

        [Test]
        [TestCase(null)]
        [TestCase(BatchType.Logged)]
        [TestCase(BatchType.Unlogged)]
        public void DeleteBatch(BatchType? batchType)
        {
            BatchStatement statement = null;
            var session = GetSession<BatchStatement>(new RowSet(), stmt => statement = stmt);
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.StringValue, cm => cm.WithName("val"))
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");

            var batch = batchType.HasValue ? session.CreateBatch(batchType.Value) : session.CreateBatch();

            const int deleteCount = 3;
            var table = GetTable<AllTypesEntity>(session, map);
            var deleteGuids = Enumerable.Range(0, deleteCount).Select(_ => Guid.NewGuid()).ToList();
            var deleteCqls = deleteGuids.Select(guid => table.Where(_ => _.UuidValue == guid).Delete());
            batch.Append(deleteCqls);

            batch.Execute();
            
            Assert.NotNull(statement);
            Assert.AreEqual(batchType ?? BatchType.Logged, statement.BatchType);
            Assert.AreEqual(deleteGuids.Count, statement.Queries.Count);

            foreach (var deleteGuid in deleteGuids)
            {
                var deleteStatement = statement.Queries.First(_ => _.QueryValues?.First() as Guid? == deleteGuid) as SimpleStatement;
                Assert.IsNotNull(deleteStatement);
                Assert.IsNotNull(deleteStatement.QueryValues);
                Assert.AreEqual(1, deleteStatement.QueryValues.Length);
                Assert.AreEqual("DELETE FROM tbl1 WHERE id = ?", deleteStatement.QueryString);
            }
        }

        [Test]
        [TestCase(null)]
        [TestCase(BatchType.Logged)]
        [TestCase(BatchType.Unlogged)]
        public void UpdateBatch(BatchType? batchType)
        {
            BatchStatement statement = null;
            var session = GetSession<BatchStatement>(new RowSet(), stmt => statement = stmt);
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.StringValue, cm => cm.WithName("val"))
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");

            var batch = batchType.HasValue ? session.CreateBatch(batchType.Value) : session.CreateBatch();

            const int updateCount = 3;
            var table = GetTable<AllTypesEntity>(session, map);
            var updateGuids = Enumerable.Range(0, updateCount).Select(_ => Guid.NewGuid()).ToList();
            var updateCqls = updateGuids.Select(guid => table
                .Where(_ => _.UuidValue == guid)
                .Select(_ => new AllTypesEntity { StringValue = "newStringFor" + guid })
                .Update());
            batch.Append(updateCqls);

            batch.Execute();

            Assert.NotNull(statement);
            Assert.AreEqual(batchType ?? BatchType.Logged, statement.BatchType);
            Assert.AreEqual(updateGuids.Count, statement.Queries.Count);

            foreach (var updateGuid in updateGuids)
            {
                var updateStatement = statement.Queries.First(_ => _.QueryValues.Length == 2 && _.QueryValues[1] as Guid? == updateGuid) as SimpleStatement;
                Assert.IsNotNull(updateStatement);
                Assert.IsNotNull(updateStatement.QueryValues);
                Assert.AreEqual(2, updateStatement.QueryValues.Length);
                Assert.AreEqual("newStringFor" + updateGuid, updateStatement.QueryValues[0]);
                Assert.AreEqual("UPDATE tbl1 SET val = ? WHERE id = ?", updateStatement.QueryString);
            }
        }
    }
}
