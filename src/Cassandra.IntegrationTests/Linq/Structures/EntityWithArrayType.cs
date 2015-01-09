using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            Table<EntityWithArrayType> table = new Table<EntityWithArrayType>(session, config);
            table.Create();

            List<EntityWithArrayType> entityList = GetDefaultEntityList();
            //Insert some data
            foreach (var singleEntity in entityList)
                table.Insert(singleEntity).Execute();

            return new Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>>(table, entityList);
        }

        public static List<EntityWithArrayType> GetDefaultEntityList()
        {
            List<EntityWithArrayType> entityList = new List<EntityWithArrayType>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                entityList.Add(GetRandomInstance(i));
            }
            return entityList;
        }

        public static EntityWithArrayType GetRandomInstance(int seed = 1)
        {
            EntityWithArrayType entity = new EntityWithArrayType();
            entity.Id = Guid.NewGuid().ToString();
            entity.ArrayType = new string[] { seed.ToString() };
            return entity;
        }

        public EntityWithArrayType Clone()
        {
            EntityWithArrayType entity = new EntityWithArrayType();
            entity.Id = Id;
            List<string> strList = new List<string>();
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
