//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using Cassandra.Data.Linq;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.CqlFunctions.Structures
{
    [Table("EntityWithNullableTimeUuid")]
    [AllowFiltering]
    public class EntityWithNullableTimeUuid
    {
        private const int DefaultRecordCount = 6;

        [PartitionKey]
        [Column("string_type")]
        public string StringType = "someStringVal";

        [Column("guid_type")]
        public Guid GuidType { get; set; }

        [Column("time_uuid_type")]
        [ClusteringKey(1)]
        public TimeUuid? NullableTimeUuidType { get; set; }

        public static void AssertEquals(EntityWithNullableTimeUuid expectedEntity, EntityWithNullableTimeUuid actualEntity)
        {
            Assert.AreEqual(expectedEntity.GuidType, actualEntity.GuidType);
            Assert.AreEqual(expectedEntity.StringType, actualEntity.StringType);
            Assert.AreEqual(expectedEntity.NullableTimeUuidType, actualEntity.NullableTimeUuidType);
        }

        public static bool AssertListContains(List<EntityWithNullableTimeUuid> expectedEntities, EntityWithNullableTimeUuid actualEntity)
        {
            foreach (var expectedEntity in expectedEntities)
            {
                try
                {
                    AssertEquals(expectedEntity, actualEntity);
                    return true;
                }
                catch (AssertionException) { }
            }
            return false;
        }

        public static List<EntityWithNullableTimeUuid> GetDefaultObjectList()
        {
            List<EntityWithNullableTimeUuid> defaultTimeUuidObjList = new List<EntityWithNullableTimeUuid>();
            for (int i = 1; i <= DefaultRecordCount; i++)
            {
                EntityWithNullableTimeUuid entity = new EntityWithNullableTimeUuid();
                entity.NullableTimeUuidType = TimeUuid.NewId(DateTimeOffset.Parse("2014-3-" + i));
                entity.GuidType = Guid.NewGuid();
                defaultTimeUuidObjList.Add(entity);
            }
            return defaultTimeUuidObjList;
        }

        public static void SetupEntity(Table<EntityWithNullableTimeUuid> tableEntityWithTimeUuid, List<EntityWithNullableTimeUuid> expectedTimeUuidObjectList)
        {
            //Insert some data
            foreach (var expectedObj in expectedTimeUuidObjectList)
                tableEntityWithTimeUuid.Insert(expectedObj).Execute();
        }

    }
}
