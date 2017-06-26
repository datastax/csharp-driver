using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqToCqlFunctionTests : MappingTestBase
    {
        [Test]
        public void MaxTimeUuid_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl100"));
            table.Where(t => t.UuidValue <= CqlFunction.MaxTimeUuid(DateTimeOffset.Parse("1/1/2005"))).Execute();
            Assert.AreEqual("SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM tbl100 WHERE UuidValue <= maxtimeuuid(?)", query);
            Assert.AreEqual(DateTimeOffset.Parse("1/1/2005"), parameters[0]);

            table.Where(t => CqlFunction.MaxTimeUuid(DateTimeOffset.Parse("1/1/2005")) > t.UuidValue).Execute();
            Assert.AreEqual("SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM tbl100 WHERE maxtimeuuid(?) > UuidValue", query);
        }

        [Test]
        public void MinTimeUuid_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl2"));
            var timestamp = DateTimeOffset.Parse("1/1/2010");
            table.Where(t => t.UuidValue < CqlFunction.MinTimeUuid(timestamp)).Execute();
            Assert.AreEqual("SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM tbl2 WHERE UuidValue < mintimeuuid(?)", query);
            Assert.AreEqual(timestamp, parameters[0]);
        }

        [Test]
        public void Token_Function_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl3").CaseSensitive());
            var key = "key1";
            table.Where(t => CqlFunction.Token(t.StringValue) > CqlFunction.Token(key)).Execute();
            Assert.AreEqual(@"SELECT ""BooleanValue"", ""DateTimeValue"", ""DecimalValue"", ""DoubleValue"", ""Int64Value"", ""IntValue"", ""StringValue"", ""UuidValue"" FROM ""tbl3"" WHERE token(""StringValue"") > token(?)", query);
            Assert.AreEqual(key, parameters[0]);
            table.Where(t => CqlFunction.Token(t.StringValue, t.Int64Value) <= CqlFunction.Token(key, "key2")).Execute();
            Assert.AreEqual(@"SELECT ""BooleanValue"", ""DateTimeValue"", ""DecimalValue"", ""DoubleValue"", ""Int64Value"", ""IntValue"", ""StringValue"", ""UuidValue"" FROM ""tbl3"" WHERE token(""StringValue"", ""Int64Value"") <= token(?, ?)", query);
            Assert.AreEqual(key, parameters[0]);
            Assert.AreEqual("key2", parameters[1]);
        }

        [Test]
        public void Append_Operator_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<CollectionTypesEntity>(session, new Map<CollectionTypesEntity>().TableName("tbl").Column(t => t.Scores, cm => cm.WithName("score_values")));
            table
                .Select(t => new CollectionTypesEntity { Scores = CqlOperator.Append(new List<int> { 5, 6 }) })
                .Where(t => t.Id == 1L)
                .Update()
                .Execute();
            Assert.AreEqual("UPDATE tbl SET score_values = score_values + ? WHERE Id = ?", query);
            CollectionAssert.AreEqual(new object[] { new List<int> { 5, 6 }, 1L }, parameters);
        }

        [Test]
        public void Prepend_Operator_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<CollectionTypesEntity>(session, new Map<CollectionTypesEntity>().TableName("tbl"));
            table
                .Select(t => new CollectionTypesEntity { Scores = CqlOperator.Prepend(new List<int> { 50, 60 }) })
                .Where(t => t.Id == 10L)
                .Update()
                .Execute();
            Assert.AreEqual("UPDATE tbl SET Scores = ? + Scores WHERE Id = ?", query);
            CollectionAssert.AreEqual(new object[] { new List<int> { 50, 60 }, 10L }, parameters);
        }

        [Test]
        public void SubstractAssign_Operator_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<CollectionTypesEntity>(session, new Map<CollectionTypesEntity>().TableName("tbl"));
            table
                .Select(t => new CollectionTypesEntity { Tags = CqlOperator.SubstractAssign(new[] { "clock" }) })
                .Where(t => t.Id == 100L)
                .Update()
                .Execute();
            Assert.AreEqual("UPDATE tbl SET Tags = Tags - ? WHERE Id = ?", query);
            CollectionAssert.AreEqual(new object[] { new [] { "clock" }, 100L }, parameters);
        }
    }
}