//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using Dse.Data.Linq;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.Structures
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

            var entityList = GetDefaultEntityList();
            //Insert some data
            foreach (var singleEntity in entityList)
                table.Insert(singleEntity).Execute();

            return new Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>>(table, entityList);
        }

        public static List<EntityWithDictionaryType> GetDefaultEntityList()
        {
            var entityList = new List<EntityWithDictionaryType>();
            for (var i = 0; i < DefaultListLength; i++)
            {
                entityList.Add(GetRandomInstance(i));
            }
            return entityList;
        }

        public static EntityWithDictionaryType GetRandomInstance(int seed = 1)
        {
            var entity = new EntityWithDictionaryType
            {
                Id = Guid.NewGuid().ToString(),
                DictionaryType = new Dictionary<string, string>() {{"key_" + seed, "val_" + seed}}
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
