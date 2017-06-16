using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    public class LinqToCqlDeleteTests : MappingTestBase
    {
        [Test]
        public void Delete_If_Test()
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
                .Column(t => t.DoubleValue, cm => cm.WithName("val1"))
                .Column(t => t.StringValue, cm => cm.WithName("val2"))
                .Column(t => t.IntValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.IntValue)
                .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            var appliedInfo = table.Where(t => t.IntValue == 100).DeleteIf(t => t.StringValue == "some value").Execute();
            Assert.AreEqual("DELETE FROM tbl1 WHERE id = ? IF val2 = ?", query);
            CollectionAssert.AreEqual(parameters, new object[] { 100, "some value" });
            //By default applied info will say it was applied
            Assert.True(appliedInfo.Applied);
        }

        [Test]
        public void Delete_With_Keyspace_Defined_Test()
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
            table.Where(t => t.IntValue == 100).Delete().Execute();
            Assert.AreEqual("DELETE FROM ks1.tbl1 WHERE id = ?", query);
        }
    }
}