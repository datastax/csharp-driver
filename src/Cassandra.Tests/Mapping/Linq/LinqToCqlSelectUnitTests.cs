using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Mapping.FluentMapping;
using Cassandra.Tests.Mapping.Pocos;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqToCqlSelectUnitTests
    {
        private static ISession GetSession(Action<string, object[]> callback)
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.ToTask(new RowSet()))
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
    }
}
