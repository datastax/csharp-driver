//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    public class Select : SimulacronTest
    {
        private List<AllDataTypesEntity> _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();
        private string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<AllDataTypesEntity> _table;

        public override void SetUp()
        {
            base.SetUp();
            Session.ChangeKeyspace(_uniqueKsName);

            _table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqSelect_SelectAll_Sync(bool async)
        {
            var table = _table;
            AllDataTypesEntity.PrimeRangeSelect(TestCluster, _entityList);

            List<AllDataTypesEntity> allEntities =
                async
                    ? table.Select(m => m).ExecuteAsync().GetAwaiter().GetResult().ToList()
                    : table.Select(m => m).Execute().ToList();
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"boolean_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "boolean_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.BooleanType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"date_time_offset_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "date_time_offset_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.DateTimeOffsetType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"date_time_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "date_time_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.DateTimeType }).ToArray())));
            List<AllDataTypesEntity> actualEntities = _table.Select(e => new AllDataTypesEntity { DateTimeType = e.DateTimeType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, actualEntities.Count);
            foreach (var entity in _entityList)
            {
                AllDataTypesEntity expectedEntity = new AllDataTypesEntity
                {
                    DateTimeType = entity.DateTimeType
                };
                AllDataTypesEntityUtil.AssertListContains(actualEntities, expectedEntity);
            }
        }

        [Test]
        public void LinqSelect_NullableDateTimeType_Sync()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"nullable_date_time_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { ("nullable_date_time_type", DataType.Timestamp) },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.NullableDateTimeType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"decimal_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "decimal_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.DecimalType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"map_type_string_long_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "map_type_string_long_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.DictionaryStringLongType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"map_type_string_string_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "map_type_string_string_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.DictionaryStringStringType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"double_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "double_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.DoubleType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"float_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "float_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.FloatType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"guid_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "guid_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.GuidType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"int64_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "int64_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.Int64Type }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"int_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "int_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.IntType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"list_of_guids_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "list_of_guids_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.ListOfGuidsType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"list_of_strings_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "list_of_strings_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.ListOfStringsType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"nullable_int_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { ("nullable_int_type", DataType.GetDataType(typeof(int?))) },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.NullableIntType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"nullable_time_uuid_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { ("nullable_time_uuid_type", DataType.GetDataType(typeof(TimeUuid?))) },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.NullableTimeUuidType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"time_uuid_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "time_uuid_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.TimeUuidType }).ToArray())));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"string_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "string_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.StringType }).ToArray())));
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

        //"SELECT " +
        //"\"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
        //"\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\"," +
        //" \"int_type\", \"int64_type\", \"list_of_guids_type\", \"list_of_strings_type\"," +
        //" \"map_type_string_long_type\", \"map_type_string_string_type\", \"nullable_date_time_type\"," +
        //" \"nullable_int_type\", \"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\" " +
        //"FROM \"allDataTypes\" " +
        //"ALLOW FILTERING";


        //public Guid Id { get; set; }

        //public string Name { get; set; }

        //public DateTimeOffset PublishingDate { get; set; }

        //public List<Song> Songs { get; set; }

        /// Tests the mapper when projecting to a new type.
        ///
        /// @jira_ticket CSHARP-414
        /// @expected_result The properties should be projected correctly
        ///
        /// @test_category linq:projection
        [Test]
        public void LinqSelect_Project_To_New_Type()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"string_type\", \"guid_type\" FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(
                          new[] { "string_type", "guid_type" },
                          rows => rows.WithRows(_entityList.Select(e => new object[] { e.StringType, e.GuidType }).ToArray())));
            var result = _table.Select(e => new Album { Name = e.StringType, Id = e.GuidType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, result.Count);

            var first = _entityList.FirstOrDefault();

            //Double checking if the properties were filled correctly
            var fetchedAlbum = result.FirstOrDefault(row => row.Name == first.StringType);

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
            var table = Session.GetTable<TestMapper>();
            const int pk = 1;
            const string value1 = "lorem ipsum";
            const string value2 = "ipsum lorem";
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT \"Col1\", \"Col2\" FROM \"TestMapper\" WHERE \"Id\" = ?",
                          when => when.WithParam(pk))
                      .ThenRowsSuccess(
                          new[] { "Col1", "Col2" },
                          rows => rows.WithRow(value1, value2)));

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
            var table = Session.GetTable<TestMapper>();

            const int pk = 2;
            const string value1 = "lorem ipsum";
            const string value2 = "ipsum lorem";

            TestCluster.PrimeFluent(
                builder => builder.WhenQuery("SELECT \"Col1\", \"Col2\" FROM \"TestMapper\" WHERE \"Id\" = ?",
                          when => when.WithParam(pk))
                      .ThenRowsSuccess(
                          new[] { "Col1", "Col2" },
                          rows => rows.WithRow(value1, value2)));

            var b = (from row in table where row.Id == pk select new TestClassBothCtors(row.Col1, row.Col2)).Execute().First();
            Assert.AreEqual(value1, b.S1);
            Assert.AreEqual(value2, b.S2);
            Assert.IsNull(b.Col1);

            TestCluster.PrimeFluent(
                builder => builder.WhenQuery("SELECT \"Col1\" FROM \"TestMapper\" WHERE \"Id\" = ?",
                                      when => when.WithParam(pk))
                                  .ThenRowsSuccess(
                                      new[] { "Col1" },
                                      rows => rows.WithRow(value1)));

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
            var table = Session.GetTable<TestMapper>();

            const int pk = 3;
            const string value1 = "lorem ipsum";

            TestCluster.PrimeFluent(
                builder => builder.WhenQuery("SELECT \"Col1\" FROM \"TestMapper\" WHERE \"Id\" = ?",
                                      when => when.WithParam(pk))
                                  .ThenRowsSuccess(
                                      new[] { "Col1" },
                                      rows => rows.WithRow(value1)));

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