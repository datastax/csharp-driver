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
        public void Create_With_Static()
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
                .Column(t => t.StringValue, cm => cm.WithName("name").AsStatic())
                .Column(t => t.IntValue, cm => cm.WithName("item_id"))
                .Column(t => t.DecimalValue, cm => cm.WithName("value"))
                .ClusteringKey("item_id")
                .TableName("items_by_id")
                .ExplicitColumns();
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.That(createQuery, Contains.Substring("name text static"));
        }

        [Test]
        public void Create_With_Secondary_Index()
        {
            var createQueries = new List<string>();
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(createQueries.Add);
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

        [Test]
        public void Create_With_Compact_Storage()
        {
            string createQuery = null;
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.UuidValue)
                .Column(t => t.UuidValue, cm => cm.WithName("user_id"))
                .Column(t => t.StringValue, cm => cm.WithName("name"))
                .Column(t => t.IntValue, cm => cm.WithName("city_id"))
                .TableName("tbl1")
                .CompactStorage()
                .ExplicitColumns();
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE tbl1 (city_id int, name text, user_id uuid, PRIMARY KEY (user_id)) WITH COMPACT STORAGE", createQuery);
        }

        [Test]
        public void Create_With_Fully_Qualified_Table_Name_Case_Sensitive()
        {
            string createQuery = null;
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.UuidValue)
                .Column(t => t.UuidValue, cm => cm.WithName("user_id"))
                .Column(t => t.StringValue, cm => cm.WithName("name"))
                .Column(t => t.IntValue, cm => cm.WithName("city_id"))
                .TableName("TBL1")
                .KeyspaceName("ks1")
                .CaseSensitive()
                .ExplicitColumns();
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE ""ks1"".""TBL1"" (""city_id"" int, ""name"" text, ""user_id"" uuid, PRIMARY KEY (""user_id""))", createQuery);
        }

        [Test]
        public void Create_With_Fully_Qualified_Table_Name_Case_Insensitive()
        {
            var createQueries = new List<string>();
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(createQueries.Add);
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.UuidValue)
                .Column(t => t.UuidValue, cm => cm.WithName("user_id"))
                .Column(t => t.StringValue, cm => cm.WithName("name"))
                .Column(t => t.IntValue, cm => cm.WithName("city_id").WithSecondaryIndex())
                .TableName("tbl2")
                .KeyspaceName("KS2")
                .ExplicitColumns();
            var table = sessionMock.Object.GetTable<AllTypesDecorated>(typeDefinition);
            table.Create();
            //keyspace.table in table creation
            Assert.AreEqual(@"CREATE TABLE KS2.tbl2 (city_id int, name text, user_id uuid, PRIMARY KEY (user_id))", createQueries[0]);

            //keyspace.table in index creation
            Assert.AreEqual(@"CREATE INDEX ON KS2.tbl2 (city_id)", createQueries[1]);
        }

        [Test]
        public void Create_With_Linq_Decorated()
        {
            var createQueries = new List<string>();
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(createQueries.Add);
            var table = sessionMock.Object.GetTable<LinqDecoratedCaseInsensitiveEntity>();
            table.Create();
            //keyspace.table in table creation
            Assert.AreEqual(@"CREATE TABLE tbl1 (i_id bigint, val1 text, val2 text, Date timestamp, PRIMARY KEY (i_id))", createQueries[0]);
            //keyspace.table in index creation
            Assert.AreEqual(@"CREATE INDEX ON tbl1 (val2)", createQueries[1]);
        }

        [Test]
        public void Create_With_Static_Column_Linq_Decorated()
        {
            var createQueries = new List<string>();
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(createQueries.Add);

            var table = sessionMock.Object.GetTable<LinqDecoratedEntityWithStaticField>();

            table.Create();

            Assert.That(createQueries, Is.Not.Empty);
            Assert.That(createQueries[0], Is.EqualTo("CREATE TABLE Items (Key int, KeyName text static, ItemId int, Value decimal, PRIMARY KEY (Key, ItemId))"));
        }

        [Test]
        public void Create_With_Varint()
        {
            string createQuery = null;
            var sessionMock = new Mock<ISession>();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var typeDefinition = new Map<VarintPoco>()
                .PartitionKey(t => t.Id)
                .TableName("tbl1");
            var table = sessionMock.Object.GetTable<VarintPoco>(typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE tbl1 (Id uuid, Name text, VarintValue varint, PRIMARY KEY (Id))", createQuery);
        }
    }
}
