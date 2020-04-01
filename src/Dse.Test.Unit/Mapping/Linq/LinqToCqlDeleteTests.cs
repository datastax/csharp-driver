//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Data.Linq;
using Dse.Mapping;
using Dse.Test.Unit.Mapping.Pocos;
using NUnit.Framework;

namespace Dse.Test.Unit.Mapping.Linq
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

        [Test]
        public void Delete_With_Query_Trace_Defined()
        {
            TestQueryTrace(table =>
            {
                var linqQuery = table.Where(x => x.IntValue == 1)
                                     .Delete();
                linqQuery.EnableTracing();
                linqQuery.Execute();
                return linqQuery.QueryTrace;
            });
        }
    }
}