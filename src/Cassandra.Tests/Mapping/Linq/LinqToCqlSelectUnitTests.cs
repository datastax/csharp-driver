using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Mapping.FluentMapping;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqToCqlSelectUnitTests
    {
        private static ISession GetSession(Action<string, object[]> callback, RowSet rs = null)
        {
            if (rs == null)
            {
                rs = new RowSet();
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.ToTask(rs))
                .Callback<BoundStatement>(stmt => callback(stmt.PreparedStatement.Cql, stmt.QueryValues))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(new PreparedStatement(null, null, query, null)))
                .Verifiable();
            return sessionMock.Object;
        }

        [Test]
        public void Select_AllowFiltering_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .Column(t => t.DecimalValue, cm => cm.WithName("val2"))
                .Column(t => t.StringValue)
                .PartitionKey(t => t.UuidValue)
                .TableName("values");

            var table = session.GetTable<AllTypesEntity>(map);
            table.Where(t => t.DecimalValue > 100M).AllowFiltering().Execute();
            Assert.AreEqual("SELECT * FROM values WHERE val2 > ? ALLOW FILTERING", query);
            Assert.AreEqual(parameters.Length, 1);
            Assert.AreEqual(parameters[0], 100M);

            table.AllowFiltering().Execute();
            Assert.AreEqual("SELECT * FROM values ALLOW FILTERING", query);
            Assert.AreEqual(0, parameters.Length);
        }

        [Test]
        public void Select_Project_To_New_Type_Constructor()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            }, TestDataHelper.CreateMultipleValuesRowSet(new[] { "val", "id" }, new[] { 1, 200 }));
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                .Column(t => t.IntValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");
            var table = session.GetTable<AllTypesEntity>(map);
            (from t in table where t.IntValue == 200 select new Tuple<double, int>(t.DoubleValue, t.IntValue)).Execute();
            Assert.AreEqual("SELECT val, id FROM tbl1 WHERE id = ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { 200 });
        }

        [Test]
        public void Select_Project_To_New_Type()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            }, TestDataHelper.CreateMultipleValuesRowSet(new[] { "val", "id" }, new[] { 1, 200 }));
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                .Column(t => t.IntValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");
            var table = session.GetTable<AllTypesEntity>(map);
            (from t in table where t.IntValue == 200 select new PlainUser {Age = t.IntValue}).Execute();
            Assert.AreEqual("SELECT id FROM tbl1 WHERE id = ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { 200 });
        }

        [Test]
        public void Select_Expression_OrderBy_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .Column(t => t.DecimalValue, cm => cm.WithName("val2"))
                .Column(t => t.StringValue, cm => cm.WithName("string_val"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");
            var id = Guid.NewGuid();
            var table = session.GetTable<AllTypesEntity>(map);
            (from t in table where t.UuidValue == id orderby t.StringValue descending select t).Execute();
            Assert.AreEqual("SELECT * FROM tbl1 WHERE id = ? ORDER BY string_val DESC", query);
            CollectionAssert.AreEqual(parameters, new object[] { id });

            (from t in table where t.UuidValue == id orderby t.StringValue select t).Execute();
            Assert.AreEqual("SELECT * FROM tbl1 WHERE id = ? ORDER BY string_val", query);
            CollectionAssert.AreEqual(parameters, new object[] { id });
        }
    }
}
