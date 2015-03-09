using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.Tests
{
    [TestFixture]
    public class StatementTests
    {
        private const string Query = "SELECT * ...";

        private static PreparedStatement GetPrepared(string query = Query, RowSetMetadata metadata = null, int protocolVersion = 3)
        {
            return new PreparedStatement(metadata, new byte[0], query, null, null, protocolVersion);
        }

        [Test]
        public void SimpleStatement_Constructor_Positional_Values()
        {
            var stmt = new SimpleStatement(Query, 1, "value 2", 3L);
            CollectionAssert.AreEqual(new object[] { 1, "value 2", 3L }, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Bind_Positional_Values()
        {
            var stmt = new SimpleStatement(Query).Bind(1, "value 2", 10030L);
            CollectionAssert.AreEqual(new object[] { 1, "value 2", 10030L }, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Constructor_No_Values()
        {
            var stmt = new SimpleStatement(Query);
            Assert.AreEqual(null, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Bind_No_Values()
        {
            var stmt = new SimpleStatement(Query).Bind();
            Assert.AreEqual(new object[0], stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Constructor_Named_Values()
        {
            var values = new { Name = "Futurama", Description = "In Stereo where available", Time = DateTimeOffset.Parse("1963-08-28") };
            var stmt = new SimpleStatement(Query, values);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(values.Name, actualValues["name"]);
            Assert.AreEqual(values.Description, actualValues["description"]);
            Assert.AreEqual(values.Time, actualValues["time"]);
        }

        [Test]
        public void SimpleStatement_Bind_Named_Values()
        {
            var values = new { Name = "Futurama", Description = "In Stereo where available", Time = DateTimeOffset.Parse("1963-08-28") };
            var stmt = new SimpleStatement(Query).Bind(values);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(values.Name, actualValues["name"]);
            Assert.AreEqual(values.Description, actualValues["description"]);
            Assert.AreEqual(values.Time, actualValues["time"]);
        }

        [Test]
        public void Statement_SetPagingState_Disables_AutoPage()
        {
            var statement = new SimpleStatement();
            Assert.True(statement.AutoPage);
            statement.SetPagingState(new byte[0]);
            Assert.False(statement.AutoPage);
            Assert.NotNull(statement.PagingState);
        }

        [Test]
        public void PreparedStatement_Bind_SetsRoutingKey_Single()
        {
            const int protocolVersion = 2;
            var metadata = new RowSetMetadata(null)
            {
                Columns = new[]
                {
                    new CqlColumn { Name = "name" }, 
                    new CqlColumn { Name = "id" }
                }
            };
            var ps = GetPrepared("SELECT * FROM tbl1 WHERE name = ? and id = ?", metadata, protocolVersion);
            ps.SetPartitionKeys(new[] { new TableColumn() { Name = "id" } });
            //The routing key is at position 1
            CollectionAssert.AreEqual(new[] { 1 }, ps.RoutingIndexes);
            Assert.Null(ps.RoutingKey);
            var bound = ps.Bind("dummy name", 1000);
            Assert.NotNull(bound.RoutingKey);
            CollectionAssert.AreEqual(TypeCodec.Encode(protocolVersion, 1000), bound.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void PreparedStatement_Bind_SetsRoutingKey_Multiple()
        {
            const int protocolVersion = 2;
            var metadata = new RowSetMetadata(null)
            {
                Columns = new[]
                {
                    new CqlColumn { Name = "id2" },
                    new CqlColumn { Name = "id1" }
                }
            };
            var ps = GetPrepared("SELECT * FROM tbl1 WHERE id2 = ? and id1", metadata, protocolVersion);
            ps.SetPartitionKeys(new[] { new TableColumn() { Name = "id1" }, new TableColumn() { Name = "id2" } });
            //The routing key is formed by the parameters at position 1 and 0
            CollectionAssert.AreEqual(new[] { 1, 0 }, ps.RoutingIndexes);
            Assert.Null(ps.RoutingKey);
            var bound = ps.Bind(2001, 1001);
            Assert.NotNull(bound.RoutingKey);
            var expectedRoutingKey = new byte[0]
                .Concat(new byte[] {0, 4})
                .Concat(TypeCodec.Encode(protocolVersion, 1001))
                .Concat(new byte[] {0})
                .Concat(new byte[] {0, 4})
                .Concat(TypeCodec.Encode(protocolVersion, 2001))
                .Concat(new byte[] {0});
            CollectionAssert.AreEqual(expectedRoutingKey, bound.RoutingKey.RawRoutingKey);
        }
    }
}
