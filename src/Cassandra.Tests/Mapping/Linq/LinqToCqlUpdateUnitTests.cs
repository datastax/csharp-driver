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
            Assert.AreEqual("UPDATE tbl1 SET string_val = ? WHERE id = ? AND val2 > ?", query);
            CollectionAssert.AreEqual(new object[] { "Billy the Vision", id, 20M }, parameters);
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
            Assert.AreEqual("UPDATE tbl1 SET HairColor = ? WHERE UserId = ?", query);
            CollectionAssert.AreEqual(new object[] { (int)HairColor.Red, id}, parameters);
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
            Assert.AreEqual("UPDATE tbl1 SET HairColor = ? WHERE UserId = ?", query);
            CollectionAssert.AreEqual(new object[] { HairColor.Red.ToString(), id }, parameters);
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
            Assert.AreEqual("UPDATE SomeKS.tbl1 SET string_val = ? WHERE id = ?", query);
            CollectionAssert.AreEqual(new object[] { "Aṣa", id }, parameters);
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
            Assert.AreEqual(
                @"UPDATE ""atd"" SET ""datetime_VALUE"" = ? WHERE ""boolean_VALUE"" = ? AND ""double_VALUE"" > ? IF ""int_VALUE"" = ?",
                query);
            CollectionAssert.AreEqual(new object[] { dateTimeValue, true, 1d, 100 }, parameters);
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
            Assert.AreEqual(
                @"UPDATE ""atd"" SET ""datetime_VALUE"" = ?, ""string_VALUE"" = ?, ""int64_VALUE"" = ? WHERE ""int_VALUE"" = ? AND ""boolean_VALUE"" = ? AND ""double_VALUE"" > ?",
                query);
            CollectionAssert.AreEqual(new object[] { dateTimeValue, dateTimeValue.ToString(), anon.Prop1, 100, true, 1d }, parameters);
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
            Assert.AreEqual(
                @"UPDATE Song SET Title = ?, Artist = ?, ReleaseDate = ? WHERE Id = ?",
                query);
            CollectionAssert.AreEqual(new object[] { other.Artist, other.Artist, DateTimeOffset.MinValue, Guid.Empty }, parameters);
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
            Assert.AreEqual(
                @"UPDATE Song SET Artist = ?, ReleaseDate = ? WHERE Id = ?",
                query);
            CollectionAssert.AreEqual(new object[] { "The Rolling Stones".ToUpperInvariant(), new DateTimeOffset(new DateTime(1999, 12, 31)), Guid.Empty }, parameters);
        }

        [Test]
        public void Update_With_Attribute_Based_Mapping()
        {
            string query = null;
            var session = GetSession((q, v) => query = q);
            var table = new Table<AttributeMappingClass>(session, new MappingConfiguration());
            table.Where(x => x.PartitionKey == 1 && x.ClusteringKey0 == 10L).Select(x => new AttributeMappingClass
            {
                DecimalValue = 10M        
            }).Update().Execute();
            Assert.AreEqual("UPDATE attr_mapping_class_table SET decimal_value_col = ? WHERE partition_key = ? AND clustering_key_0 = ?", query);
        }
    }
}