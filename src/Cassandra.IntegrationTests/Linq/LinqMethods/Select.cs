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

        [Test]
        public void LinqSelect_Project_To_New_Type()
        {
            var result = _table.Select(e => new Album { Name = e.StringType, Id = e.GuidType }).Execute().ToList();
            Assert.AreEqual(_entityList.Count, result.Count);
            Assert.AreNotEqual(Guid.Empty, result.First().Id);
        }
    }
}
