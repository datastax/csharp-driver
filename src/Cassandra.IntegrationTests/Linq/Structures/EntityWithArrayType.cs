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
    class EntityWithArrayType
    {
        private const int DefaultListLength = 5;
        public string Id { get; set; }
        public string[] ArrayType { get; set; }

        public static Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> SetupDefaultTable(ISession session)
        {
            // create table
            var config = new MappingConfiguration().Define(
                new Map<EntityWithArrayType>()
                .TableName("EntityWithArrayType_" + Randomm.RandomAlphaNum(12))
                .PartitionKey(u => u.Id));
            var table = new Table<EntityWithArrayType>(session, config);
            table.Create();

            var entityList = EntityWithArrayType.GetDefaultEntityList();
            //Insert some data
            foreach (var singleEntity in entityList)
                table.Insert(singleEntity).Execute();

            return new Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>>(table, entityList);
        }
        
        public static Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> GetDefaultTable(
            ISession session, string tableName)
        {
            // create table
            var config = new MappingConfiguration().Define(
                new Map<EntityWithArrayType>()
                    .TableName(tableName)
                    .PartitionKey(u => u.Id));
            var table = new Table<EntityWithArrayType>(session, config);
            var entityList = EntityWithArrayType.GetDefaultEntityList();

            return new Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>>(table, entityList);
        }

        public static List<EntityWithArrayType> GetDefaultEntityList()
        {
            var entityList = new List<EntityWithArrayType>();
            for (var i = 0; i < EntityWithArrayType.DefaultListLength; i++)
            {
                entityList.Add(EntityWithArrayType.GetRandomInstance(i));
            }
            return entityList;
        }

        public static EntityWithArrayType GetRandomInstance(int seed = 1)
        {
            var entity = new EntityWithArrayType();
            entity.Id = Guid.NewGuid().ToString();
            entity.ArrayType = new string[] { seed.ToString() };
            return entity;
        }

        public EntityWithArrayType Clone()
        {
            var entity = new EntityWithArrayType();
            entity.Id = Id;
            var strList = new List<string>();
            strList.AddRange(ArrayType);
            entity.ArrayType = strList.ToArray();
            return entity;
        }

        public void AssertEquals(EntityWithArrayType actualEntity)
        {
            Assert.AreEqual(Id, actualEntity.Id);
            Assert.AreEqual(ArrayType, actualEntity.ArrayType);
        }
    }
}
