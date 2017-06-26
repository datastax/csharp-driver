using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Serialization;
using Cassandra.Tests.Mapping.Pocos;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqCreateTableUnitTests : MappingTestBase
    {
        [Test]
        public void Create_With_Composite_Partition_Key()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.StringValue, t => t.TimeUuidValue)
                .Column(t => t.IntValue, cm => cm.WithName("int_value"));
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
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
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .PartitionKey(t => t.StringValue, t => t.IntValue)
                .Column(t => t.IntValue, cm => cm.WithName("int_value"))
                .ClusteringKey("DateTimeValue");
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE AllTypesDecorated " +
                            "(BooleanValue boolean, DateTimeValue timestamp, DecimalValue decimal, DoubleValue double, " +
                            "Int64Value bigint, int_value int, StringValue text, TimeUuidValue timeuuid, UuidValue uuid, " +
                            "PRIMARY KEY ((StringValue, int_value), DateTimeValue))", createQuery);
        }

        [Test]
        public void Create_With_Fluent_Clustering_Key()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .Column(t => t.IntValue, cm => cm.WithName("int_value"))
                .Column(t => t.Int64Value, cm => cm.WithName("long_value"))
                .PartitionKey(t => t.StringValue)
                .ClusteringKey("DateTimeValue")
                .ClusteringKey(t => t.Int64Value);
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE AllTypesDecorated " +
                            "(BooleanValue boolean, DateTimeValue timestamp, DecimalValue decimal, DoubleValue double, " +
                            "long_value bigint, int_value int, StringValue text, TimeUuidValue timeuuid, UuidValue uuid, " +
                            "PRIMARY KEY (StringValue, DateTimeValue, long_value))", createQuery);
        }

        [Test]
        public void Create_With_Fluent_Clustering_Key_And_Clustering_Order()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q)
                .Verifiable();
            var typeDefinition = new Map<AllTypesDecorated>()
                .Column(t => t.IntValue, cm => cm.WithName("int_value"))
                .Column(t => t.Int64Value, cm => cm.WithName("long_value"))
                .PartitionKey(t => t.StringValue)
                .ClusteringKey(t => t.Int64Value, SortOrder.Descending)
                .ClusteringKey(t => t.DateTimeValue);
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE AllTypesDecorated " +
                            "(BooleanValue boolean, DateTimeValue timestamp, DecimalValue decimal, DoubleValue double, " +
                            "long_value bigint, int_value int, StringValue text, TimeUuidValue timeuuid, UuidValue uuid, " +
                            "PRIMARY KEY (StringValue, long_value, DateTimeValue)) WITH CLUSTERING ORDER BY (long_value DESC)", createQuery);
        }

        [Test]
        public void Create_With_Composite_Partition_Key_And_Clustering_Key_Explicit()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
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
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE AllTypesDecorated " +
                            "(bigint_value bigint, int_value int, StringValue text, TimeUuidValue timeuuid, " +
                            "PRIMARY KEY ((StringValue, int_value), TimeUuidValue))", createQuery);
        }

        [Test]
        public void Create_With_Counter()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
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
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual("CREATE TABLE item_visits (visits counter, id uuid, PRIMARY KEY (id))", createQuery);
        }

        [Test]
        public void Create_With_Static()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
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
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.That(createQuery, Contains.Substring("name text static"));
        }

        [Test]
        public void Create_With_Secondary_Index()
        {
            var createQueries = new List<string>();
            var sessionMock = GetSessionMock();
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
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE ""USERS"" (""city_id"" int, ""name"" text, ""user_id"" uuid, PRIMARY KEY (""user_id""))", createQueries[0]);
            Assert.AreEqual(@"CREATE INDEX ON ""USERS"" (""city_id"")", createQueries[1]);
        }

        [Test]
        public void Create_With_Compact_Storage()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
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
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE tbl1 (city_id int, name text, user_id uuid, PRIMARY KEY (user_id)) WITH COMPACT STORAGE", createQuery);
        }

        [Test]
        public void Create_With_Fully_Qualified_Table_Name_Case_Sensitive()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
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
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE ""ks1"".""TBL1"" (""city_id"" int, ""name"" text, ""user_id"" uuid, PRIMARY KEY (""user_id""))", createQuery);
        }

        [Test]
        public void Create_With_Fully_Qualified_Table_Name_Case_Insensitive()
        {
            var createQueries = new List<string>();
            var sessionMock = GetSessionMock();
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
            var table = GetTable<AllTypesDecorated>(sessionMock.Object, typeDefinition);
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
            var sessionMock = GetSessionMock();
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
            var sessionMock = GetSessionMock();
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
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var typeDefinition = new Map<VarintPoco>()
                .PartitionKey(t => t.Id)
                .TableName("tbl1");
            var table = GetTable<VarintPoco>(sessionMock.Object, typeDefinition);
            table.Create();
            Assert.AreEqual(@"CREATE TABLE tbl1 (Id uuid, Name text, VarintValue varint, PRIMARY KEY (Id))", createQuery);
        }

        [Test]
        public void Create_With_Ignored_Prop()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var table = sessionMock.Object.GetTable<LinqDecoratedEntity>();
            table.Create();
            //It contains Ignored props: Ignored1 and Ignored2
            Assert.AreEqual(@"CREATE TABLE ""x_t"" (""x_pk"" text, ""x_ck1"" int, ""x_ck2"" int, ""x_f1"" int, PRIMARY KEY (""x_pk"", ""x_ck1"", ""x_ck2""))", createQuery);
        }

        [Test]
        public void Create_With_MappingDecorated_TimeSeries()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var definition = new Cassandra.Mapping.Attributes.AttributeBasedTypeDefinition(typeof(DecoratedTimeSeries));
            var table = GetTable<DecoratedTimeSeries>(sessionMock.Object, definition);
            table.Create();
            //It contains Ignored props: Ignored1 and Ignored2
            Assert.AreEqual(@"CREATE TABLE ""ks1"".""tbl1"" (""name"" text, ""Slice"" int, ""Time"" timeuuid, ""val"" double, ""Value2"" text, PRIMARY KEY ((""name"", ""Slice""), ""Time""))", createQuery);
        }

        [Test]
        public void Create_With_Collections()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var definition = new Map<CollectionTypesEntity>()
                .PartitionKey(c => c.Id)
                .Column(c => c.Tags, cm => cm.WithDbType<SortedSet<string>>())
                .TableName("tbl1");
            var table = GetTable<CollectionTypesEntity>(sessionMock.Object, definition);
            table.Create();
            Assert.AreEqual("CREATE TABLE tbl1 (Id bigint, Scores list<int>, Tags set<text>, Favs map<text, text>, PRIMARY KEY (Id))", createQuery);
        }

        [Test]
        public void Create_With_Tuple()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var definition = new Map<UdtAndTuplePoco>()
                .PartitionKey(c => c.Id1)
                .Column(c => c.Id1, cm => cm.WithName("id"))
                .Column(c => c.Tuple1, cm => cm.WithName("position"))
                .ExplicitColumns()
                .TableName("tbl1");
            var table = GetTable<UdtAndTuplePoco>(sessionMock.Object, definition);
            table.Create();
            Assert.AreEqual("CREATE TABLE tbl1 (id uuid, position tuple<bigint, bigint, text>, PRIMARY KEY (id))", createQuery);
        }

        [Test]
        public void Create_With_Frozen_Udt()
        {
            string createQuery = null;
            var serializer = new Serializer(ProtocolVersion.MaxSupported);
            var sessionMock = GetSessionMock(serializer);
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var definition = new Map<UdtAndTuplePoco>()
                .PartitionKey(c => c.Id1)
                .Column(c => c.Id1, cm => cm.WithName("id"))
                .Column(c => c.Udt1, cm => cm.WithName("my_udt").WithDbType<Song>().AsFrozen())
                .ExplicitColumns()
                .TableName("tbl1");
            var udtInfo = new UdtColumnInfo("ks1.song");
            udtInfo.Fields.Add(new ColumnDesc { Name = "title", TypeCode = ColumnTypeCode.Ascii });
            udtInfo.Fields.Add(new ColumnDesc { Name = "releasedate", TypeCode = ColumnTypeCode.Timestamp });
            var udtMap = UdtMap.For<Song>().SetIgnoreCase(false);
            udtMap.SetSerializer(serializer);
            udtMap.Build(udtInfo);
            serializer.SetUdtMap("song", udtMap);
            var table = GetTable<UdtAndTuplePoco>(sessionMock.Object, definition);
            table.Create();
            Assert.AreEqual("CREATE TABLE tbl1 (id uuid, my_udt frozen<\"ks1\".\"song\">, PRIMARY KEY (id))", createQuery);
        }

        [Test]
        public void Create_With_Frozen_Tuple()
        {
            string createQuery = null;
            var sessionMock = GetSessionMock();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var definition = new Map<UdtAndTuplePoco>()
                .PartitionKey(c => c.Id1)
                .Column(c => c.Id1, cm => cm.WithName("id"))
                .Column(c => c.Tuple1, cm => cm.WithName("position").AsFrozen())
                .ExplicitColumns()
                .TableName("tbl1");
            var table = GetTable<UdtAndTuplePoco>(sessionMock.Object, definition);
            table.Create();
            Assert.AreEqual("CREATE TABLE tbl1 (id uuid, position frozen<tuple<bigint, bigint, text>>, PRIMARY KEY (id))", createQuery);
        }

        [Test]
        public void Create_With_Frozen_Collection_Key()
        {
            string createQuery = null;
            var serializer = new Serializer(ProtocolVersion.MaxSupported);
            var sessionMock = GetSessionMock(serializer);
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var definition = new Map<UdtAndTuplePoco>()
                .PartitionKey(c => c.Id1)
                .Column(c => c.Id1, cm => cm.WithName("id"))
                .Column(c => c.UdtSet1, cm => cm.WithName("my_set").WithDbType<SortedSet<Song>>().WithFrozenKey())
                .Column(c => c.TupleMapKey1, cm => cm.WithName("my_map").WithFrozenKey())
                .ExplicitColumns()
                .TableName("tbl1");
            var udtInfo = new UdtColumnInfo("song");
            udtInfo.Fields.Add(new ColumnDesc { Name = "title", TypeCode = ColumnTypeCode.Ascii });
            udtInfo.Fields.Add(new ColumnDesc { Name = "releasedate", TypeCode = ColumnTypeCode.Timestamp });
            var udtMap = UdtMap.For<Song>();
            udtMap.SetSerializer(serializer);
            udtMap.Build(udtInfo);
            serializer.SetUdtMap("song", udtMap);
            var table = GetTable<UdtAndTuplePoco>(sessionMock.Object, definition);
            table.Create();
            Assert.AreEqual("CREATE TABLE tbl1 (id uuid, my_set set<frozen<\"song\">>, my_map map<frozen<tuple<double, double>>, text>, PRIMARY KEY (id))", createQuery);
        }

        [Test]
        public void Create_With_Frozen_Collection_Value()
        {
            string createQuery = null;
            var serializer = new Serializer(ProtocolVersion.MaxSupported);
            var sessionMock = GetSessionMock(serializer);
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var definition = new Map<UdtAndTuplePoco>()
                .PartitionKey(c => c.Id1)
                .Column(c => c.Id1, cm => cm.WithName("id"))
                .Column(c => c.UdtList1, cm => cm.WithName("my_list").WithFrozenValue())
                .Column(c => c.TupleMapValue1, cm => cm.WithName("my_map").WithFrozenValue())
                .ExplicitColumns()
                .TableName("tbl1");
            var udtInfo = new UdtColumnInfo("song");
            udtInfo.Fields.Add(new ColumnDesc { Name = "title", TypeCode = ColumnTypeCode.Ascii });
            udtInfo.Fields.Add(new ColumnDesc { Name = "releasedate", TypeCode = ColumnTypeCode.Timestamp });
            var udtMap = UdtMap.For<Song>();
            udtMap.SetSerializer(serializer);
            udtMap.Build(udtInfo);
            serializer.SetUdtMap("song", udtMap);
            var table = GetTable<UdtAndTuplePoco>(sessionMock.Object, definition);
            table.Create();
            Assert.AreEqual("CREATE TABLE tbl1 (id uuid, my_list list<frozen<\"song\">>, my_map map<text, frozen<tuple<double, double>>>, PRIMARY KEY (id))", createQuery);
        }

        [Test]
        public void Create_With_Attribute_Defined_Mappings()
        {
            string createQuery = null;
            var serializer = new Serializer(ProtocolVersion.MaxSupported);
            var sessionMock = GetSessionMock(serializer);
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>()))
                .Returns(() => new RowSet())
                .Callback<string>(q => createQuery = q);
            var table = new Table<AttributeMappingClass>(sessionMock.Object, new MappingConfiguration());
            table.Create();
            Assert.AreEqual("CREATE TABLE attr_mapping_class_table (partition_key int, clustering_key_0 bigint, clustering_key_1 text, clustering_key_2 uuid, bool_value_col boolean, float_value_col float, decimal_value_col decimal, PRIMARY KEY (partition_key, clustering_key_0, clustering_key_1, clustering_key_2)) WITH CLUSTERING ORDER BY (clustering_key_0 ASC, clustering_key_1 ASC, clustering_key_2 DESC)", createQuery);
        }

        private static Mock<ISession> GetSessionMock(Serializer serializer = null)
        {
            if (serializer == null)
            {
                serializer = new Serializer(ProtocolVersion.MaxSupported);
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            var config = new Configuration();
            var metadata = new Metadata(config);
            var ccMock = new Mock<IMetadataQueryProvider>(MockBehavior.Strict);
            ccMock.Setup(cc => cc.Serializer).Returns(serializer);
            metadata.ControlConnection = ccMock.Object;
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.Metadata).Returns(metadata);
            sessionMock.Setup(s => s.Cluster).Returns(clusterMock.Object);
            return sessionMock;
        }
    }
}