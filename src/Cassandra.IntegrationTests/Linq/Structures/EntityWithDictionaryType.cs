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
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Linq.Structures
{
    class EntityWithDictionaryType
    {
        private const int DefaultListLength = 5;
        public string Id { get; set; }
        public Dictionary<string, string> DictionaryType { get; set; }

        public static Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>> SetupDefaultTable(ISession session)
        {
            // create table
            var config = new MappingConfiguration().Define(
                new Map<EntityWithDictionaryType>()
                .TableName($"EntityWithDictionaryType_{Randomm.RandomAlphaNum(12)}")
                .PartitionKey(u => u.Id));
            var table = new Table<EntityWithDictionaryType>(session, config);
            table.Create();

            var entityList = EntityWithDictionaryType.GetDefaultEntityList();
            //Insert some data
            foreach (var singleEntity in entityList)
                table.Insert(singleEntity).Execute();

            return new Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>>(table, entityList);
        }

        public static Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>> GetDefaultTable(
            ISession session, string tableName)
        {
            // create table
            var config = new MappingConfiguration().Define(
                new Map<EntityWithDictionaryType>()
                    .TableName(tableName)
                    .PartitionKey(u => u.Id));
            var table = new Table<EntityWithDictionaryType>(session, config);

            var entityList = EntityWithDictionaryType.GetDefaultEntityList();

            return new Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>>(table, entityList);
        }

        public static List<EntityWithDictionaryType> GetDefaultEntityList()
        {
            var entityList = new List<EntityWithDictionaryType>();
            for (var i = 0; i < EntityWithDictionaryType.DefaultListLength; i++)
            {
                entityList.Add(EntityWithDictionaryType.GetRandomInstance(i));
            }
            return entityList;
        }

        public static EntityWithDictionaryType GetRandomInstance(int seed = 1)
        {
            var entity = new EntityWithDictionaryType
            {
                Id = Guid.NewGuid().ToString(),
                DictionaryType = new Dictionary<string, string>() { { "key_" + seed, "val_" + seed } }
            };
            return entity;
        }

        public EntityWithDictionaryType Clone()
        {
            var entity = new EntityWithDictionaryType
            {
                Id = Id,
                DictionaryType = new Dictionary<string, string>(DictionaryType)
            };
            return entity;
        }

        public void AssertEquals(EntityWithDictionaryType actualEntity)
        {
            Assert.AreEqual(Id, actualEntity.Id);
            Assert.AreEqual(DictionaryType, actualEntity.DictionaryType);
        }
    }
}
