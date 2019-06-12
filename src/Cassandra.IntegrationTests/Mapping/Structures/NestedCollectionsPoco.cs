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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Mapping.Structures
{
    [AllowFiltering]
    public class NestedCollectionsPoco
    {
        public const int DefaultListLength = 5;

        public string StringType { get; set; }
        public Guid GuidType { get; set; }
        public Dictionary<string, Dictionary<string, long>> NestedDictionaryDictionary { get; set; }
        public List<Dictionary<string, string>> NestedListDictionary { get; set; }
        public SortedList<string, Dictionary<string, DateTime>> NestedSortedListDictionary { get; set; }

        public static NestedCollectionsPoco GetRandomInstance()
        {
            Dictionary<string, Dictionary<string, long>> nestedDictionaryDictionary = new Dictionary<string, Dictionary<string, long>>();
            nestedDictionaryDictionary.Add(Randomm.RandomAlphaNum(10), new Dictionary<string, long>() { { Randomm.RandomAlphaNum(10), 123456789L } });
            List<Dictionary<string, string>> nestedListDictionary = new List<Dictionary<string, string>>();
            nestedListDictionary.Add(new Dictionary<string, string>() { { Randomm.RandomAlphaNum(10), Randomm.RandomAlphaNum(10) }, { Randomm.RandomAlphaNum(10), Randomm.RandomAlphaNum(10) } });
            nestedListDictionary.Add(new Dictionary<string, string>() { { Randomm.RandomAlphaNum(10), Randomm.RandomAlphaNum(10) }, { Randomm.RandomAlphaNum(10), Randomm.RandomAlphaNum(10) } });
            SortedList<string, Dictionary<string, DateTime>> nestedSortedListDictionary = new SortedList<string, Dictionary<string, DateTime>>();
            nestedSortedListDictionary.Add("abc", new Dictionary<string, DateTime>() { { Randomm.RandomAlphaNum(10), DateTime.Now.AddDays(1) } });
            nestedSortedListDictionary.Add("zyz", new Dictionary<string, DateTime>() { { Randomm.RandomAlphaNum(10), DateTime.Now.AddHours(900) } });
            nestedSortedListDictionary.Add("def", new Dictionary<string, DateTime>() { { Randomm.RandomAlphaNum(10), DateTime.Now.AddHours(1) } });

            NestedCollectionsPoco randomInstance = new NestedCollectionsPoco
            {
                StringType = "StringType_val_" + Randomm.RandomAlphaNum(10),
                GuidType = Guid.NewGuid(),
                NestedDictionaryDictionary = nestedDictionaryDictionary,
                NestedListDictionary = nestedListDictionary,
                NestedSortedListDictionary = nestedSortedListDictionary,
            };
            return randomInstance;
        }

        public void AssertEquals(NestedCollectionsPoco poco)
        {
            Assert.AreEqual(StringType, poco.StringType);
            Assert.AreEqual(GuidType, poco.GuidType);
            CollectionAssert.AreEqual(NestedDictionaryDictionary, poco.NestedDictionaryDictionary);
            CollectionAssert.AreEqual(NestedListDictionary, poco.NestedListDictionary);
            CollectionAssert.AreEqual(NestedSortedListDictionary, poco.NestedSortedListDictionary);
        }

        public static List<NestedCollectionsPoco> GetDefaultAllDataTypesList()
        {
            List<NestedCollectionsPoco> movieList = new List<NestedCollectionsPoco>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                movieList.Add(GetRandomInstance());
            }
            return movieList;
        }

        public static MappingConfiguration GetDefaultMappingConfig()
        {
            var config = (new Map<NestedCollectionsPoco>().PartitionKey(c => c.StringType)).CaseSensitive();
            return new MappingConfiguration().Define(config);
        }

        public static List<NestedCollectionsPoco> SetupDefaultTable(ISession session)
        {
            // drop table if exists, re-create
            var table = new Table<NestedCollectionsPoco>(session, GetDefaultMappingConfig());
            table.Create();

            List<NestedCollectionsPoco> allDataTypesRandomList = GetDefaultAllDataTypesList();
            //Insert some data
            foreach (var allDataTypesEntity in allDataTypesRandomList)
                table.Insert(allDataTypesEntity).Execute();

            return allDataTypesRandomList;
        }

        public static bool ListContains(List<NestedCollectionsPoco> expectedEntities, NestedCollectionsPoco actualEntity)
        {
            foreach (var expectedEntity in expectedEntities)
            {
                try
                {
                    expectedEntity.AssertEquals(actualEntity);
                    return true;
                }
                catch (AssertionException) { }
            }
            return false;
        }

        public static void AssertListContains(List<NestedCollectionsPoco> expectedEntities, NestedCollectionsPoco actualEntity)
        {
            Assert.IsTrue(ListContains(expectedEntities, actualEntity));
        }

        public static void AssertListEqualsList(List<NestedCollectionsPoco> expectedEntities, List<NestedCollectionsPoco> actualEntities)
        {
            Assert.AreEqual(expectedEntities.Count, actualEntities.Count);
            foreach (var expectedEntity in expectedEntities)
                Assert.IsTrue(ListContains(actualEntities, expectedEntity));
        }


        /// <summary>
        /// Test Assertion helper that will try the SELECT query a few times in case we need to wait for consistency
        /// </summary>
        public static void KeepTryingSelectAndAssert(IMapper mapper, string selectStatement, List<NestedCollectionsPoco> expectedInstanceList)
        {
            List<NestedCollectionsPoco> instancesQueried = mapper.Fetch<NestedCollectionsPoco>(selectStatement).ToList();
            DateTime futureDateTime = DateTime.Now.AddSeconds(5);
            while (instancesQueried.Count < expectedInstanceList.Count && DateTime.Now < futureDateTime)
            {
                Thread.Sleep(50);
                instancesQueried = mapper.Fetch<NestedCollectionsPoco>(selectStatement).ToList();
            }
            AssertListEqualsList(expectedInstanceList, instancesQueried);
        }

    }
}