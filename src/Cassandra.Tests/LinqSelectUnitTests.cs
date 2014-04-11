using Cassandra.Data.Linq;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace Cassandra.Tests
{
    [TestFixture]
    public class LinqSelectUnitTests
    {
        [Test]
        public void LinqSelectTest()
        {
            var sessionMock = new Mock<ISession>();
            var session = sessionMock.Object;

            var ctx = new Context(session);
            var entity = new AllTypesEntity();

            ctx.AddTable<AllTypesEntity>();
            ContextTable<AllTypesEntity> table = ctx.GetTable<AllTypesEntity>();
            var date = new DateTime(1975, 1, 1);
            var linqQueries = new List<CqlQuery<AllTypesEntity>>()
            {
                (from ent in table where ent.BooleanValue == true select ent),
                (from ent in table where ent.BooleanValue == false select ent),
                (from ent in table where ent.DateTimeValue < date select ent),
                (from ent in table where ent.DateTimeValue >= date select ent),
                (from ent in table where ent.IntValue == 0 select ent),
                (from ent in table where ent.StringValue == "Hello world" select ent)
                
            };
            var expectedCqlQueries = new List<string>()
            {
                "SELECT * FROM \"AllTypesEntity\" WHERE \"BooleanValue\" = true",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"BooleanValue\" = false",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"DateTimeValue\" < 157766400000",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"DateTimeValue\" >= 157766400000",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"IntValue\" = 0",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"StringValue\" = 'Hello world'"
            };
            var actualCqlQueries = new List<IStatement>();
            sessionMock
                .Setup(s => s.BeginExecute(It.IsAny<IStatement>(), It.IsAny<object>(), It.IsAny<AsyncCallback>(), It.IsAny<object>()))
                .Callback<IStatement, object, AsyncCallback, object>((stmt2, b, c, d) => actualCqlQueries.Add(stmt2));

            
            //Execute all linq queries
            foreach (var q in linqQueries)
            {
                q.Execute();
            }
            sessionMock.Verify();

            Assert.AreEqual(expectedCqlQueries.Count, actualCqlQueries.Count);
            //Check that all expected queries and actual queries are equal
            for (var i = 0; i < expectedCqlQueries.Count; i++ )
            {
                Assert.IsInstanceOf<SimpleStatement>(actualCqlQueries[i]);
                Assert.AreEqual(
                    expectedCqlQueries[i], 
                    ((SimpleStatement)actualCqlQueries[i]).QueryString,
                    "Expected Cql query and generated CQL query by Linq do not match.");
            }
        }
    }

    /// <summary>
    /// Test utility: Represents an application entity with most of common types as properties
    /// </summary>
    public class AllTypesEntity
    {
        public bool BooleanValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public decimal DecimalValue { get; set; }
        public double DoubleValue { get; set; }
        public Int64 Int64Value { get; set; }
        public int IntValue { get; set; }
        public string StringValue { get; set; }
        public Guid UuidValue { get; set; }
    }
}
