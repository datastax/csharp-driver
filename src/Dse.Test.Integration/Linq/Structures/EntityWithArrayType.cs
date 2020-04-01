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
