//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Data.Linq;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.Structures
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
