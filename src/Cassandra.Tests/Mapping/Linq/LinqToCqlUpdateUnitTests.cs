using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
            var table = GetTable<AllTypesEntity>(session, map);
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
            var table = GetTable<AllTypesEntity>(session, map);
            table
                .Where(t => t.UuidValue == id)
                .Where(t => t.DecimalValue > 20M)
                .Select(t => new AllTypesEntity { StringValue = "Billy the Vision" })
                .Update()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns("tbl1", query, new []{"string_val"},
                new [] {"id", "val2"}, new object[] {"Billy the Vision", id, 20M},
                parameters);
        }

        [Test]
        public void Update_Set_Enum_Int()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var map = new Map<PlainUser>()
                .Column(t => t.HairColor, cm => cm.WithDbType<int>())
                .PartitionKey(t => t.UserId)
                .TableName("tbl1");
            var id = Guid.NewGuid();
            var table = GetTable<PlainUser>(session, map);
            table
                .Where(t => t.UserId == id)
                .Select(t => new PlainUser { HairColor = HairColor.Red })
                .Update()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns("tbl1", query, new []{"HairColor"},
                new [] {"UserId"}, new object[] { (int)HairColor.Red, id},
                parameters);
        }

        [Test]
        public void Update_Set_Enum_String()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var map = new Map<PlainUser>()
                .Column(t => t.HairColor, cm => cm.WithDbType<string>())
                .PartitionKey(t => t.UserId)
                .TableName("tbl1");
            var id = Guid.NewGuid();
            var table = GetTable<PlainUser>(session, map);
            table
                .Where(t => t.UserId == id)
                .Select(t => new PlainUser { HairColor = HairColor.Red })
                .Update()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns("tbl1", query, new []{"HairColor"},
                new [] {"UserId"}, new object[] { HairColor.Red.ToString(), id},
                parameters);
        }

        [Test]
        public void Update_With_Query_Trace_Defined()
        {
            TestQueryTrace(table =>
            {
                var linqQuery = table.Where(x => x.IntValue == 1)
                                     .Select(x => new AllTypesEntity { StringValue = "a"})
                                     .Update();
                linqQuery.EnableTracing();
                linqQuery.Execute();
                return linqQuery.QueryTrace;
            });
        }

        [Test]
        public void Update_With_Keyspace_Defined_Test()
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
                .KeyspaceName("SomeKS")
                .TableName("tbl1");
            var id = Guid.NewGuid();
            var table = GetTable<AllTypesEntity>(session, map);
            table
                .Where(t => t.UuidValue == id)
                .Select(t => new AllTypesEntity { StringValue = "Aṣa" })
                .Update()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns("SomeKS.tbl1", query, new []{"string_val"},
                new [] {"id"}, new object[] { "Aṣa", id },
                parameters);
        }

        [Test]
        public void Update_If_Exists()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var id = Guid.NewGuid();
            var table = GetTable<Song>(session, new Map<Song>()
                .ExplicitColumns()
                .Column(t => t.Title, cm => cm.WithName("title"))
                .Column(t => t.Id, cm => cm.WithName("id"))
                .PartitionKey(t => t.Id)
                .TableName("songs"));

            // IF EXISTS
            table
                .Where(t => t.Id == id)
                .Select(t => new Song { Title = "When The Sun Goes Down" })
                .UpdateIfExists()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns("songs", query, new []{"title"},
                new [] {"id"}, new object[] { "When The Sun Goes Down", id },
                parameters, "IF EXISTS");
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
            TestHelper.VerifyUpdateCqlColumns(@"""atd""", query, new []{@"""string_VALUE"""},
                new [] {@"""boolean_VALUE""", @"""double_VALUE"""}, new object[] {"updated value", true, 1d, 100},
                parameters, "IF \"int_VALUE\" = ?");
        }

        [Test]
        public void UpdateIf_Set_From_Variable_With_Where_Clause()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = session.GetTable<AllTypesDecorated>();
            var dateTimeValue = DateTime.Now;
            table
                .Where(t => t.BooleanValue == true && t.DoubleValue > 1d)
                .Select(t => new AllTypesDecorated { DateTimeValue = dateTimeValue })
                .UpdateIf(t => t.IntValue == 100)
                .Execute();
            TestHelper.VerifyUpdateCqlColumns(@"""atd""", query, new []{@"""datetime_VALUE"""},
                new [] {@"""boolean_VALUE""", @"""double_VALUE"""}, new object[] {dateTimeValue, true, 1d, 100},
                parameters, "IF \"int_VALUE\" = ?");
        }

        [Test]
        public void Update_Set_From_Variable_With_Where_Clause()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = session.GetTable<AllTypesDecorated>();
            var dateTimeValue = DateTime.Now;
            var anon = new { Prop1 = 1L };
            table
                .Where(t => t.IntValue == 100 && t.BooleanValue == true && t.DoubleValue > 1d)
                .Select(t => new AllTypesDecorated
                {
                    DateTimeValue = dateTimeValue,
                    StringValue = dateTimeValue.ToString(),
                    Int64Value = anon.Prop1
                })
                .Update()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns(@"""atd""", query, new []{@"""datetime_VALUE""", @"""string_VALUE""", @"""int64_VALUE"""},
                new [] {@"""int_VALUE""", @"""boolean_VALUE""", @"""double_VALUE"""},
                new object[] {dateTimeValue, dateTimeValue.ToString(), anon.Prop1, 100, true, 1d},
                parameters);
        }

        [Test]
        public void Update_Set_From_Other_Instances_With_Where_Clause()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = new Table<Song>(session);
            var other = new Song()
            {
                Id = Guid.NewGuid(),
                Artist = "The Rolling Stones",
                Title = "Paint It Black"
            };
            table
                .Where(t => t.Id == Guid.Empty)
                .Select(t => new Song
                {
                    Title = other.Artist,
                    Artist = other.Artist,
                    ReleaseDate = DateTimeOffset.MinValue
                })
                .Update()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"Title", "Artist", "ReleaseDate"},
                new [] {"Id"}, new object[] {other.Artist, other.Artist, DateTimeOffset.MinValue, Guid.Empty},
                parameters);
        }

        [Test]
        public void Update_Set_From_New_Instance_Expression_With_Where_Clause()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = new Table<Song>(session);
            table
                .Where(t => t.Id == Guid.Empty)
                .Select(t => new Song
                {
                    Artist = Convert.ToString("The Rolling Stones").ToUpperInvariant(),
                    ReleaseDate = new DateTimeOffset(new DateTime(1999, 12, 31))
                })
                .Update()
                .Execute();
            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"Artist", "ReleaseDate"},
                new [] {"Id"}, new object[] {"The Rolling Stones".ToUpperInvariant(), new DateTimeOffset(new DateTime(1999, 12, 31)), Guid.Empty},
                parameters);
        }

        [Test]
        public void Update_With_Attribute_Based_Mapping()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) => {
                query = q;
                parameters = v;
            });
            var table = new Table<AttributeMappingClass>(session, new MappingConfiguration());
            table.Where(x => x.PartitionKey == 1 && x.ClusteringKey0 == 10L).Select(x => new AttributeMappingClass
            {
                DecimalValue = 10M        
            }).Update().Execute();
            TestHelper.VerifyUpdateCqlColumns("attr_mapping_class_table", query, new []{"decimal_value_col"},
                new [] {"partition_key", "clustering_key_0"}, new object[] {10M, 1, 10L}, parameters);
        }

        [Test]
        public void Update_Dictionary_With_Substract_Assign()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var map = new Map<CollectionTypesEntity>()
                .ExplicitColumns()
                .PartitionKey(x => x.Id)
                .Column(x => x.Id, cm => cm.WithName("id"))
                .Column(x => x.Favs, cm => cm.WithName("favs"))
                .TableName("tbl1");
            var table = GetTable<CollectionTypesEntity>(session, map);
            var id = 100L;
            table.Where(x => x.Id == id)
                 .Select(x => new CollectionTypesEntity { Favs = x.Favs.SubstractAssign("a", "b", "c")})
                 .Update().Execute();
            Assert.AreEqual("UPDATE tbl1 SET favs = favs - ? WHERE id = ?", query);
            CollectionAssert.AreEquivalent(new object[]{ new [] { "a", "b", "c" }, id }, parameters);
        }

        [Test]
        public void Update_Where_In_With_Composite_Keys()
        {
            BoundStatement statement = null;
            var session = GetSession<BoundStatement>(new RowSet(), stmt => statement = stmt);
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.IntValue, cm => cm.WithName("id3"))
                .Column(t => t.StringValue, cm => cm.WithName("id2"))
                .Column(t => t.UuidValue, cm => cm.WithName("id1"))
                .Column(t => t.Int64Value, cm => cm.WithName("val"))
                .PartitionKey(t => t.UuidValue)
                .ClusteringKey(t => t.StringValue, SortOrder.Ascending)
                .ClusteringKey(t => t.IntValue, SortOrder.Descending)
                .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            const string expectedQuery = "UPDATE tbl1 SET val = ? WHERE id1 = ? AND (id2, id3) IN ?";
            var id = Guid.NewGuid();
            var value = 100L;
            var list = new List<Tuple<string, int>> {Tuple.Create("z", 1)};
            // Using Tuple.Create()
            table.Where(t => t.UuidValue == id && list.Contains(Tuple.Create(t.StringValue, t.IntValue)))
                 .Select(t => new AllTypesEntity { Int64Value = value })
                 .Update().Execute();
            Assert.NotNull(statement);
            CollectionAssert.AreEquivalent(new object[] {value, id, list }, statement.QueryValues);
            Assert.AreEqual(expectedQuery, statement.PreparedStatement.Cql);
            // Using constructor
            table.Where(t => t.UuidValue == id && list.Contains(new Tuple<string, int>(t.StringValue, t.IntValue)))
                 .Select(t => new AllTypesEntity { Int64Value = value })
                 .Update().Execute();
            Assert.NotNull(statement);
            CollectionAssert.AreEquivalent(new object[] {value, id, list}, statement.QueryValues);
            Assert.AreEqual(expectedQuery, statement.PreparedStatement.Cql);
        }
        
        private class TestObjectStaticProperty
        {
            public static string Property => "static";
        }

        [Test]
        public void StaticPropertyAccess_Test()
        {
            var table = new Table<LinqDecoratedWithStringCkEntity>(GetSession((_, __) => { }));

            var cql = table.Select(t => new LinqDecoratedWithStringCkEntity
            {
                pk = TestObjectStaticProperty.Property,
                ck1 = TestObjectStaticProperty.Property
            }).Update().GetCql(out var parameters);

            Assert.That(parameters, Is.EquivalentTo(new[] { "static", "static" }));
            Assert.AreEqual(@"UPDATE ""x_ts"" SET ""x_pk"" = ?, ""x_ck1"" = ?", cql);
        }
    }
}