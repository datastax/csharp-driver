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
    public class LinqCreateTableUnitTests
    {
        [Test]
        public void Create_With_Composite_Partition_Key()
        {
            string createQuery = null;
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.StringValue, t => t.TimeUuidValue)
                .Column(t => t.IntValue, cm => cm.WithName("int_value"));
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE AllTypesDecorated " +
                            "(BooleanValue boolean, DateTimeValue timestamp, DecimalValue decimal, DoubleValue double, " +
                            "Int64Value bigint, int_value int, StringValue text, TimeUuidValue timeuuid, UuidValue uuid, " +
                            "PRIMARY KEY ((StringValue, TimeUuidValue)))", createQuery);
        }

        [Test]
        public void Create_With_Composite_Partition_Key_And_Clustering_Key()
        {
            string createQuery = null;
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.StringValue, t => t.IntValue)
                .Column(t => t.IntValue, cm => cm.WithName("int_value"))
                .ClusteringKey("DateTimeValue");
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE AllTypesDecorated " +
                            "(BooleanValue boolean, DateTimeValue timestamp, DecimalValue decimal, DoubleValue double, " +
                            "Int64Value bigint, int_value int, StringValue text, TimeUuidValue timeuuid, UuidValue uuid, " +
                            "PRIMARY KEY ((StringValue, int_value), DateTimeValue))", createQuery);
        }

        [Test]
        public void Create_With_Composite_Partition_Key_And_Clustering_Key_Explicit()
        {
            string createQuery = null;
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.StringValue, t => t.IntValue)
                .Column(t => t.StringValue)
                .Column(t => t.TimeUuidValue)
                .Column(t => t.IntValue, cm => cm.WithName("int_value"))
                .Column(t => t.Int64Value, cm => cm.WithName("bigint_value"))
                .ClusteringKey("TimeUuidValue")
                .ExplicitColumns();
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE AllTypesDecorated " +
                            "(bigint_value bigint, int_value int, StringValue text, TimeUuidValue timeuuid, " +
                            "PRIMARY KEY ((StringValue, int_value), TimeUuidValue))", createQuery);
        }

        [Test]
        public void Create_With_Counter()
        {
            string createQuery = null;
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.UuidValue)
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .Column(t => t.Int64Value, cm => cm.AsCounter().WithName("visits"))
                .TableName("item_visits")
                .ExplicitColumns();
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE item_visits (visits counter, id uuid, PRIMARY KEY (id))", createQuery);
        }

        [Test]
        public void Create_With_SecondaryIndex()
        {
            var createQueries = new List<string>();
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(createQueries.Add)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.UuidValue)
                .Column(t => t.UuidValue, cm => cm.WithName("user_id"))
                .Column(t => t.StringValue, cm => cm.WithName("name"))
                .Column(t => t.IntValue, cm => cm.WithName("city_id").WithSecondaryIndex())
                .TableName("USERS")
                .CaseSensitive()
                .ExplicitColumns();
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE ""USERS"" (""city_id"" int, ""name"" text, ""user_id"" uuid, PRIMARY KEY (""user_id""))", createQueries[0]);
            Assert.AreEqual(@"CREATE INDEX ON ""USERS"" (""city_id"")", createQueries[1]);
        }
    }
}
