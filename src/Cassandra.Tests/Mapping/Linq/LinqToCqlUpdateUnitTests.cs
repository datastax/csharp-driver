using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    public class LinqToCqlUpdateUnitTests : MappingTestBase
    {
        [Test]
        public void Update_TTL_Test()
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
            table
                .Where(t => t.UuidValue == id)
                .Select(t => new AllTypesEntity { StringValue = "Billy the Vision", DecimalValue = 10M })
                .Update()
                .SetTTL(60 * 60)
                .Execute();
            Assert.AreEqual("UPDATE tbl1 USING TTL ? SET string_val = ?, val2 = ? WHERE id = ?", query);
            CollectionAssert.AreEqual(new object[] { 60 * 60, "Billy the Vision", 10M, id }, parameters);
        }

        [Test]
        public void Update_Multiple_Where_Test()
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
            table
                .Where(t => t.UuidValue == id)
                .Where(t => t.DecimalValue > 20M)
                .Select(t => new AllTypesEntity { StringValue = "Billy the Vision" })
                .Update()
                .Execute();
            Assert.AreEqual("UPDATE tbl1 SET string_val = ? WHERE id = ? AND val2 > ?", query);
            CollectionAssert.AreEqual(new object[] { "Billy the Vision", id, 20M}, parameters);
        }

        [Test]
        public void UpdateIf_With_Where_Clause()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = session.GetTable<AllTypesDecorated>();
            table
                .Where(t => t.BooleanValue == true && t.DoubleValue > 1d)
                .Select(t => new AllTypesDecorated { StringValue = "updated value" })
                .UpdateIf(t => t.IntValue == 100)
                .Execute();
            Assert.AreEqual(
                @"UPDATE ""atd"" SET ""string_VALUE"" = ? WHERE ""boolean_VALUE"" = ? AND ""double_VALUE"" > ? IF ""int_VALUE"" = ?",
                query);
            CollectionAssert.AreEqual(new object[] {"updated value", true, 1d, 100}, parameters);
        }
    }
}
