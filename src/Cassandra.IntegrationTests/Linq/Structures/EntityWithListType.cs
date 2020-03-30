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
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Structures
{
    class EntityWithListType
    {
        private const int DefaultListLength = 5;
        public string Id { get; set; }
        public List<int> ListType { get; set; }
        
        public static Tuple<Table<EntityWithListType>, List<EntityWithListType>> GetDefaultTable(
            ISession session, string tableName)
        {
            // create table
            var config = new MappingConfiguration().Define(
                new Map<EntityWithListType>()
                    .TableName(tableName)
                    .PartitionKey(u => u.Id));
            var table = new Table<EntityWithListType>(session, config);
            var entityList = EntityWithListType.GetDefaultEntityList();

            return new Tuple<Table<EntityWithListType>, List<EntityWithListType>>(table, entityList);
        }

        public static Tuple<Table<EntityWithListType>, List<EntityWithListType>> SetupDefaultTable(
            ISession session)
        {
            // create table
            var config = new MappingConfiguration().Define(
                new Map<EntityWithListType>()
                .TableName("EntityWithListType_" + Randomm.RandomAlphaNum(12))
                .PartitionKey(u => u.Id));
            var table = new Table<EntityWithListType>(session, config);
            table.Create();

            var entityList = EntityWithListType.GetDefaultEntityList();
            //Insert some data
            foreach (var singleEntity in entityList)
                table.Insert(singleEntity).Execute();

            return new Tuple<Table<EntityWithListType>, List<EntityWithListType>>(table, entityList);
        }

        public static List<EntityWithListType> GetDefaultEntityList()
        {
            var entityList = new List<EntityWithListType>();
            for (var i = 0; i < EntityWithListType.DefaultListLength; i++)
            {
                entityList.Add(EntityWithListType.GetRandomInstance(i));
            }
            return entityList;
        }

        public static EntityWithListType GetRandomInstance(int seed = 1)
        {
            var entity = new EntityWithListType();
            entity.Id = Guid.NewGuid().ToString();
            entity.ListType = new List<int>() { seed };
            return entity;
        }

        public EntityWithListType Clone()
        {
            var entity = new EntityWithListType();
            entity.Id = Id;
            entity.ListType = new List<int>();
            entity.ListType.AddRange(ListType);
            return entity;
        }

        public void AssertEquals(EntityWithListType actualEntity)
        {
            Assert.AreEqual(Id, actualEntity.Id);
            CollectionAssert.AreEquivalent(ListType, actualEntity.ListType);
        }
    }
}
