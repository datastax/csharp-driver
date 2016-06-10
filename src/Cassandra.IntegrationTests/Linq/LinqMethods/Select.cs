using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Select : SharedClusterTest
    {
        ISession _session = null;
        private List<AllDataTypesEntity> _entityList;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<AllDataTypesEntity> _table;

        protected override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _entityList = AllDataTypesEntity.SetupDefaultTable(_session);
            _table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
        }

        [Test]
        public void LinqSelect_SelectAll_Sync()
        {
            var table = _table;
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            List<AllDataTypesEntity> allEntities = table.Select(m => m).Execute().ToList();
            Assert.AreEqual(_entityList.Count, allEntities.Count);
            foreach (var entity in allEntities)
            {
                AllDataTypesEntityUtil.AssertListContains(_entityList, entity);
            }
        }

        [Test]
        public void LinqSelect_SelectAll_Async()
        {
            var table = _table;
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            List<AllDataTypesEntity> allEntities = table.Select(m => m).ExecuteAsync().Result.ToList();
            Assert.AreEqual(_entityList.Count, allEntities.Count);
            foreach (var entity in allEntities)
            {
                AllDataTypesEntityUtil.AssertListContains(_entityList, entity);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////
        /// Begin: Select every possible C* data type
        ////////////////////////////////////////////////////////////////////////////////

        [Test]
        public void LinqSelect_BooleanType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { BooleanType = e.BooleanType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    BooleanType = entity.BooleanType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_DateTimeOffsetType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { DateTimeOffsetType = e.DateTimeOffsetType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    DateTimeOffsetType = entity.DateTimeOffsetType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_DateTimeType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { DateTimeType = e.DateTimeType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    DateTimeType = entity.DateTimeType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
                Assert.AreEqual(DateTimeKind.Utc, entity.DateTimeType.Kind);
            }
        }

        [Test]
        public void LinqSelect_NullableDateTimeType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { NullableDateTimeType = e.NullableDateTimeType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    NullableDateTimeType = entity.NullableDateTimeType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_DecimalType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { DecimalType = e.DecimalType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    DecimalType = entity.DecimalType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_DictionaryStringLongType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { DictionaryStringLongType = e.DictionaryStringLongType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    DictionaryStringLongType = entity.DictionaryStringLongType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_DictionaryStringStringType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { DictionaryStringStringType = e.DictionaryStringStringType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    DictionaryStringStringType = entity.DictionaryStringStringType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_DoubleType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { DoubleType = e.DoubleType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    DoubleType = entity.DoubleType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_FloatType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { FloatType = e.FloatType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    FloatType = entity.FloatType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_GuidType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { GuidType = e.GuidType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    GuidType = entity.GuidType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_Int64Type_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { Int64Type = e.Int64Type }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    Int64Type = entity.Int64Type
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_IntType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { IntType = e.IntType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    IntType = entity.IntType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_ListOfGuidsType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { ListOfGuidsType = e.ListOfGuidsType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    ListOfGuidsType = entity.ListOfGuidsType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_ListOfStringsType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { ListOfStringsType = e.ListOfStringsType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    ListOfStringsType = entity.ListOfStringsType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_NullableIntType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { NullableIntType = e.NullableIntType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    NullableIntType = entity.NullableIntType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_NullableTimeUuidType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { NullableTimeUuidType = e.NullableTimeUuidType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    NullableTimeUuidType = entity.NullableTimeUuidType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_TimeUuidType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { TimeUuidType = e.TimeUuidType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    TimeUuidType = entity.TimeUuidType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_StringType_Sync()
        {
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { StringType = e.StringType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    StringType = entity.StringType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        /// Tests the mapper when projecting to a new type.
        /// 
        /// @jira_ticket CSHARP-414
        /// @expected_result The properties should be projected correctly
        ///
        /// @test_category linq:projection
        [Test]
        public void LinqSelect_Project_To_New_Type()
        {
            var result = _table.Select(e => new Album { Name = e.StringType, Id = e.GuidType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, result.Count);
            
            var first = _entityList.FirstOrDefault();

            //Double checking if the properties were filled correctly
            var fetchedAlbum = result.FirstOrDefault(row => row.Name == first.StringType) ;

            Assert.AreEqual(first.StringType, fetchedAlbum.Name);
            Assert.AreEqual(first.GuidType, fetchedAlbum.Id);


            Assert.AreNotEqual(Guid.Empty, fetchedAlbum.Id);
        }

        /// Tests the mapper when projecting to a new type.
        /// 
        /// Case: When the projecting type has a property with the same name as the Table, but shouldn't be projected
        ///
        /// @jira_ticket CSHARP-414
        /// @expected_result The property should be empty
        ///
        /// @test_category linq:projection
        [Test]
        public void LinqSelect_Project_To_New_Type_With_Conflict_Properties()
        {
            var table = _session.GetTable<TestMapper>();
            table.CreateIfNotExists();

            const int pk = 1;
            const string value1 = "lorem ipsum";
            const string value2 = "ipsum lorem";

            table.Insert(new TestMapper() { Col1 = value1, Col2 = value2, Id = pk }).Execute();

            var a = (from row in table where row.Id == pk select new TestClassDefaultCtor { S1 = row.Col1, S2 = row.Col2 }).Execute().First();

            Assert.AreEqual(value1, a.S1);
            Assert.AreEqual(value2, a.S2);
            Assert.IsNull(a.Col1);
        }

        /// Tests the mapper when projecting to a new type using constructor.
        /// 
        /// Case: When the projecting type has a property with the same name as the Table, but shouldn't be projected
        ///
        /// @jira_ticket CSHARP-414
        /// @expected_result The property should be empty
        ///
        /// @test_category linq:projection
        [Test]
        public void LinqSelect_Project_To_New_Type_With_Conflict_Properties_Using_Constructor()
        {
            var table = _session.GetTable<TestMapper>();
            table.CreateIfNotExists();

            const int pk = 2;
            const string value1 = "lorem ipsum";
            const string value2 = "ipsum lorem";

            table.Insert(new TestMapper() { Col1 = value1, Col2 = value2, Id = pk }).Execute();

            var b = (from row in table where row.Id == pk select new TestClassBothCtors(row.Col1, row.Col2)).Execute().First();
            Assert.AreEqual(value1, b.S1);
            Assert.AreEqual(value2, b.S2);
            Assert.IsNull(b.Col1);

            var c = (from row in table where row.Id == pk select new TestClassSingleProp(row.Col1)).Execute().First(); // throws
            Assert.NotNull(c);
            Assert.AreEqual(value1, c.S1);

        }

        /// Tests the mapper when projecting to a new type using constructor.
        /// 
        /// Case: When the projecting type has only one property with the same name as the Table
        ///
        /// @jira_ticket CSHARP-414
        /// @expected_result The property should be projected
        ///
        /// @test_category linq:projection
        [Test]
        public void LinqSelect_Project_To_New_Type_With_Only_One_Property()
        {
            var table = _session.GetTable<TestMapper>();
            table.CreateIfNotExists();

            const int pk = 3;
            const string value1 = "lorem ipsum";
            const string value2 = "ipsum lorem";

            table.Insert(new TestMapper() { Col1 = value1, Col2 = value2, Id = pk }).Execute();

            var c = (from row in table where row.Id == pk select new TestClassSingleProp(row.Col1)).Execute().First(); // throws
            Assert.NotNull(c);
            Assert.AreEqual(value1, c.S1);
        }

        [Table]
        public class TestMapper
        {
            [PartitionKey(1)]
            public int Id;

            public string Col1;
            public string Col2;
        }

        public class TestClassDefaultCtor
        {
            public string S1;
            public string S2;

            public string Col1; //same name as in the 'TestMapper' class
        }

        public class TestClassBothCtors
        {
            public string S1;
            public string S2;

            public string Col1; //same name as in the 'TestMapper' class

            public TestClassBothCtors(string s1, string s2)
            {
                this.S1 = s1;
                this.S2 = s2;
            }
        }

        public class TestClassSingleProp
        {
            public string S1;
            public TestClassSingleProp(string s1)
            {
                this.S1 = s1;
            }
        }

    }
}
