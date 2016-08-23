#if !NO_MOCKS
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    public class LinqToCqlSelectUnitTests : MappingTestBase
    {
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

            var table = GetTable<AllTypesEntity>(session, map);
            table.Where(t => t.DecimalValue > 100M).AllowFiltering().Execute();
            Assert.AreEqual("SELECT val2, val, StringValue, id FROM values WHERE val2 > ? ALLOW FILTERING", query);
            Assert.AreEqual(parameters.Length, 1);
            Assert.AreEqual(parameters[0], 100M);

            table.AllowFiltering().Execute();
            Assert.AreEqual("SELECT val2, val, StringValue, id FROM values ALLOW FILTERING", query);
            Assert.AreEqual(0, parameters.Length);
        }

        [Test]
        public void Select_Contains_Test()
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
                .Column(t => t.DecimalValue, cm => cm.WithName("val2"))
                .Column(t => t.DateTimeValue, cm => cm.WithName("d"))
                .TableName("values");

            var table = GetTable<AllTypesEntity>(session, map);
            table.Where(t => new [] {1M, 2M}.Contains(t.DecimalValue)).Select(t => new AllTypesEntity { DateTimeValue = t.DateTimeValue}).Execute();
            Assert.AreEqual("SELECT d FROM values WHERE val2 IN (?, ?)", query);
            CollectionAssert.AreEqual(new [] {1M, 2M}, parameters);
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
            var table = GetTable<AllTypesEntity>(session, map);
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
            var table = GetTable<AllTypesEntity>(session, map);
            (from t in table where t.IntValue == 200 select new PlainUser { Age = t.IntValue }).Execute();
            Assert.AreEqual("SELECT id FROM tbl1 WHERE id = ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { 200 });
        }

        [Test]
        public void Select_Project_To_Single_Type()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            }, TestDataHelper.GetSingleValueRowSet("val", 123D));
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                .Column(t => t.IntValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            const long parameter = 500L;
            var result = (from t in table where t.IntValue == parameter select t.DoubleValue).First().Execute();
            Assert.AreEqual(123D, result);
            Assert.AreEqual("SELECT val FROM tbl1 WHERE id = ? LIMIT ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { parameter, 1});

            session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            }, TestDataHelper.GetSingleValueRowSet("val", 1234D));
            table = GetTable<AllTypesEntity>(session, map);
            result = table.Where(x => x.IntValue == parameter).Select(x => x.DoubleValue).First().Execute();
            Assert.AreEqual(1234D, result);
            Assert.AreEqual("SELECT val FROM tbl1 WHERE id = ? LIMIT ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { parameter, 1 });
        }

        [Test]
        public void Select_Project_Chained()
        {
            string query = null;
            object[] parameters = null;
            BoundStatement statement = null;
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                .Column(t => t.IntValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");
            var session = GetSession(
                TestDataHelper.CreateMultipleValuesRowSet(new [] {"val", "id"}, new object[] { -1D, 10 }),
                s =>
                {
                    statement = s;
                    query = s.PreparedStatement.Cql;
                    parameters = s.QueryValues;
                });
            const long parameter = 500L;
            var table = GetTable<AllTypesEntity>(session, map);
            var result = table.Where(x => x.IntValue == parameter)
                .SetConsistencyLevel(ConsistencyLevel.All)
                .Select(x => new Tuple<double, int>(x.DoubleValue, x.IntValue))
                .Select(p => p.Item1)
                .Execute();
            Assert.AreEqual(-1D, result.First());
            Assert.AreEqual("SELECT val, id FROM tbl1 WHERE id = ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { parameter });
            //Check that properties are being maintained
            Assert.AreEqual(ConsistencyLevel.All, statement.ConsistencyLevel);
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
            var table = GetTable<AllTypesEntity>(session, map);
            (from t in table where t.UuidValue == id orderby t.StringValue descending select t).Execute();
            Assert.AreEqual("SELECT val2, val, string_val, id FROM tbl1 WHERE id = ? ORDER BY string_val DESC", query);
            CollectionAssert.AreEqual(parameters, new object[] { id });

            (from t in table where t.UuidValue == id orderby t.StringValue select t).Execute();
            Assert.AreEqual("SELECT val2, val, string_val, id FROM tbl1 WHERE id = ? ORDER BY string_val", query);
            CollectionAssert.AreEqual(parameters, new object[] { id });
        }

        [Test]
        public void Select_Expression_With_Ignored_In_Where_Clause_Throws_Test()
        {
            var session = GetSession((q, v) => { });
            var table = session.GetTable<LinqDecoratedEntity>();
            var ex = Assert.Throws<InvalidOperationException>(() => (from t in table where t.pk == "pkval" && t.Ignored1 == "aa" select t).Execute());
            StringAssert.Contains("ignored", ex.Message);
        }

        [Test]
        public void Select_Expression_With_Ignored_In_Order_Clause_Throws_Test()
        {
            var session = GetSession((q, v) => { });
            var table = session.GetTable<LinqDecoratedEntity>();
            var ex = Assert.Throws<InvalidOperationException>(() => (from t in table where t.pk == "pkval" orderby t.Ignored1 select t).Execute());
            StringAssert.Contains("ignored", ex.Message);
        }

        [Test]
        public void Select_With_Keyspace_Defined()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            }, TestDataHelper.CreateMultipleValuesRowSet(new[] { "id" }, new[] { 5000 }));
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                .Column(t => t.IntValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .CaseSensitive()
                .KeyspaceName("ks1")
                .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            (from t in table where t.IntValue == 199 select new { Age = t.IntValue }).Execute();
            Assert.AreEqual(@"SELECT ""id"" FROM ""ks1"".""tbl1"" WHERE ""id"" = ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { 199 });
        }
    }
}
#endif