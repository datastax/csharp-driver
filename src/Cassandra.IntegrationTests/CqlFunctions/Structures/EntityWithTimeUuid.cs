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
using Cassandra.Data.Linq;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using NUnit.Framework.Internal;

#pragma warning disable 618

namespace Cassandra.IntegrationTests.CqlFunctions.Structures
{
    [Table("EntityWithTimeUuid")]
    [AllowFiltering]
    public class EntityWithTimeUuid
    {
        private const int DefaultRecordCount = 6;

        [PartitionKey]
        [Column("string_type")]
        public string StringType = "someStringVal";

        [Column("guid_type")]
        public Guid GuidType { get; set; }

        [Column("time_uuid_type")]
        [ClusteringKey(1)]
        public TimeUuid TimeUuidType { get; set; }

        public static void AssertEquals(EntityWithTimeUuid expectedEntity, EntityWithTimeUuid actualEntity)
        {
            Assert.AreEqual(expectedEntity.GuidType, actualEntity.GuidType);
            Assert.AreEqual(expectedEntity.StringType, actualEntity.StringType);
            Assert.AreEqual(expectedEntity.TimeUuidType, actualEntity.TimeUuidType);
        }

        public static bool AssertListContains(List<EntityWithTimeUuid> expectedEntities, EntityWithTimeUuid actualEntity)
        {
            foreach (var expectedEntity in expectedEntities)
            {
                using (new TestExecutionContext.IsolatedContext())
                {
                    try
                    {
                        AssertEquals(expectedEntity, actualEntity);
                        return true;
                    }
                    catch (AssertionException)
                    {
                    }
                }
            }
            return false;
        }

        public static List<EntityWithTimeUuid> GetDefaultObjectList()
        {
            List<EntityWithTimeUuid> defaultTimeUuidObjList = new List<EntityWithTimeUuid>();
            for (int i = 1; i <= DefaultRecordCount; i++)
            {
                EntityWithTimeUuid entity = new EntityWithTimeUuid();
                entity.TimeUuidType = TimeUuid.NewId(DateTimeOffset.Parse("2014-3-" + i));
                entity.GuidType = Guid.NewGuid();
                defaultTimeUuidObjList.Add(entity);
            }
            return defaultTimeUuidObjList;
        }

        public static void SetupEntity(Table<EntityWithTimeUuid> tableEntityWithTimeUuid, List<EntityWithTimeUuid> expectedTimeUuidObjectList)
        {
            //Insert some data
            foreach (var expectedObj in expectedTimeUuidObjectList)
                tableEntityWithTimeUuid.Insert(expectedObj).Execute();
        }
    }
}
