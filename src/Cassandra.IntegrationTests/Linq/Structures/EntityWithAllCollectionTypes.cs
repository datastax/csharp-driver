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
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Structures
{
    public class EntityWithAllCollectionTypes
    {
        public const int DefaultListLength = 5;

        public string Id { get; set; }
        public List<int> ListType { get; set; }
        public string[] ArrayType { get; set; }
        public Dictionary<string, string> DictionaryType { get; set; }

        public static EntityWithAllCollectionTypes GetRandomInstance(int seed = 1)
        {
            EntityWithAllCollectionTypes cte = new EntityWithAllCollectionTypes();
            cte.Id = Guid.NewGuid().ToString();
            cte.ListType = new List<int>() { seed };
            cte.ArrayType = new string[] { seed.ToString() };
            cte.DictionaryType = new Dictionary<string, string>() { {"key_" + seed, "val_" + seed} };
            return cte;
        }

        public void AssertEquals(EntityWithAllCollectionTypes actualEntity)
        {
            Assert.AreEqual(Id, actualEntity.Id);
            Assert.AreEqual(ListType, actualEntity.ListType);
            Assert.AreEqual(ArrayType, actualEntity.ArrayType);
            Assert.AreEqual(DictionaryType, actualEntity.DictionaryType);
        }

        public static List<EntityWithAllCollectionTypes> GetDefaultAllDataTypesList()
        {
            List<EntityWithAllCollectionTypes> entityList = new List<EntityWithAllCollectionTypes>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                entityList.Add(GetRandomInstance(i));
            }
            return entityList;
        }

        public static Tuple<Table<EntityWithAllCollectionTypes>, List<EntityWithAllCollectionTypes>> SetupDefaultTable(ISession session)
        {
            // create table
            var config = new MappingConfiguration().Define(
                new Map<EntityWithAllCollectionTypes>()
                .TableName("EntityWithAllCollectionTypes_" + Randomm.RandomAlphaNum(12))
                .PartitionKey(u => u.Id));
            Table<EntityWithAllCollectionTypes> table = new Table<EntityWithAllCollectionTypes>(session, config);
            table.Create();

            List<EntityWithAllCollectionTypes> entityList = GetDefaultAllDataTypesList();
            //Insert some data
            foreach (var singleEntity in entityList)
                table.Insert(singleEntity).Execute();

            return new Tuple<Table<EntityWithAllCollectionTypes>, List<EntityWithAllCollectionTypes>>(table, entityList);
        }

        public EntityWithAllCollectionTypes Clone()
        {
            EntityWithAllCollectionTypes cte = new EntityWithAllCollectionTypes();
            cte.Id = Id;
            cte.ListType = new List<int>();
            cte.ListType.AddRange(ListType);
            cte.ArrayType = (string[])ArrayType.Clone();
            cte.DictionaryType = new Dictionary<string, string>();
            foreach (var fav in DictionaryType)
                cte.DictionaryType.Add(fav.Key, fav.Value);
            return cte;
        }

    }
}
