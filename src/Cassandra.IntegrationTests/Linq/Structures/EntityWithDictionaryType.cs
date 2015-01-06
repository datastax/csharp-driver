﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

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
                .TableName("EntityWithDictionaryType_" + Randomm.RandomAlphaNum(12))
                .PartitionKey(u => u.Id));
            Table<EntityWithDictionaryType> table = new Table<EntityWithDictionaryType>(session, config);
            table.Create();

            List<EntityWithDictionaryType> entityList = GetDefaultEntityList();
            //Insert some data
            foreach (var singleEntity in entityList)
                table.Insert(singleEntity).Execute();

            return new Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>>(table, entityList);
        }

        public static List<EntityWithDictionaryType> GetDefaultEntityList()
        {
            List<EntityWithDictionaryType> entityList = new List<EntityWithDictionaryType>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                entityList.Add(GetRandomInstance(i));
            }
            return entityList;
        }

        public static EntityWithDictionaryType GetRandomInstance(int seed = 1)
        {
            EntityWithDictionaryType entity = new EntityWithDictionaryType();
            entity.Id = Guid.NewGuid().ToString();
            entity.DictionaryType = new Dictionary<string, string>() { {"key_" + seed, "val_" + seed} };
            return entity;
        }

        public EntityWithDictionaryType Clone()
        {
            EntityWithDictionaryType entity = new EntityWithDictionaryType();
            entity.Id = Id;
            entity.DictionaryType = new Dictionary<string, string>(DictionaryType);
            return entity;
        }

        public void AssertEquals(EntityWithDictionaryType actualEntity)
        {
            Assert.AreEqual(Id, actualEntity.Id);
            Assert.AreEqual(DictionaryType, actualEntity.DictionaryType);
        }
    }
}
