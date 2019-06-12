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
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.Structures
{
    [AllowFiltering]
    [Table("ManyDataTypesEntity")]
    public class ManyDataTypesEntity
    {
        public const int DefaultListLength = 5;

        [PartitionKey]
        public string StringType { get; set; }
        public Guid GuidType { get; set; }
        public DateTime DateTimeType { get; set; }
        public DateTimeOffset DateTimeOffsetType { get; set; }
        public bool BooleanType { get; set; }
        public Decimal DecimalType { get; set; }
        public double DoubleType { get; set; }
        public float FloatType { get; set; }
        public int? NullableIntType { get; set; }
        public int IntType { get; set; }
        public Int64 Int64Type { get; set; }
        //public TimeUuid TimeUuidType { get; set; }
        //public TimeUuid? NullableTimeUuidType { get; set; }
        public Dictionary<string, long> DictionaryStringLongType { get; set; }
        public Dictionary<string, string> DictionaryStringStringType { get; set; }
        public List<Guid> ListOfGuidsType { get; set; }
        public List<string> ListOfStringsType { get; set; }

        public static ManyDataTypesEntity GetRandomInstance()
        {
            Dictionary<string, long> dictionaryStringLong = new Dictionary<string, long>() { { "key_" + Randomm.RandomAlphaNum(10), (long)1234321 } };
            Dictionary<string, string> dictionaryStringString = new Dictionary<string, string>() { { "key_" + Randomm.RandomAlphaNum(10), "value_" + Randomm.RandomAlphaNum(10) } };
            List<Guid> listOfGuidsType = new List<Guid>() { Guid.NewGuid(), Guid.NewGuid() };
            List<string> listOfStringsType = new List<string>() { Randomm.RandomAlphaNum(20), Randomm.RandomAlphaNum(12), "" };


            ManyDataTypesEntity randomRow = new ManyDataTypesEntity
            {
                StringType = "StringType_val_" + Randomm.RandomAlphaNum(10),
                GuidType = Guid.NewGuid(),
                DateTimeType = DateTime.Now.ToUniversalTime(),
                DateTimeOffsetType = new DateTimeOffset(),
                BooleanType = false,
                DecimalType = (decimal)98765432.0,
                DoubleType = (double)9876543,
                FloatType = (float)987654,
                NullableIntType = null,
                IntType = 98765,
                Int64Type = (Int64)9876,
                //TimeUuidType = TimeUuid.NewId(),
                //NullableTimeUuidType = null,
                DictionaryStringLongType = dictionaryStringLong,
                DictionaryStringStringType = dictionaryStringString,
                ListOfGuidsType = listOfGuidsType,
                ListOfStringsType = listOfStringsType,
            };
            return randomRow;
        }

        public void AssertEquals(ManyDataTypesEntity actualRow)
        {
            Assert.AreEqual(StringType, actualRow.StringType);
            Assert.AreEqual(GuidType, actualRow.GuidType);
            Assert.AreEqual(DateTimeType.ToString(), actualRow.DateTimeType.ToString()); // 'ToString' rounds to the nearest second
            Assert.AreEqual(DateTimeOffsetType.ToString(), actualRow.DateTimeOffsetType.ToString());
            Assert.AreEqual(BooleanType, actualRow.BooleanType);
            Assert.AreEqual(DecimalType, actualRow.DecimalType);
            Assert.AreEqual(DoubleType, actualRow.DoubleType);
            Assert.AreEqual(FloatType, actualRow.FloatType);
            Assert.AreEqual(IntType, actualRow.IntType);
            Assert.AreEqual(Int64Type, actualRow.Int64Type);
            //Assert.AreEqual(TimeUuidType, actualRow.TimeUuidType);
            //Assert.AreEqual(NullableTimeUuidType, actualRow.NullableTimeUuidType);
            Assert.AreEqual(DictionaryStringLongType, actualRow.DictionaryStringLongType);
            Assert.AreEqual(DictionaryStringStringType, actualRow.DictionaryStringStringType);
            Assert.AreEqual(ListOfGuidsType, actualRow.ListOfGuidsType);
            Assert.AreEqual(ListOfStringsType, actualRow.ListOfStringsType);
        }

        public static List<ManyDataTypesEntity> GetDefaultAllDataTypesList()
        {
            List<ManyDataTypesEntity> movieList = new List<ManyDataTypesEntity>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                movieList.Add(GetRandomInstance());
            }
            return movieList;
        }

        public static List<ManyDataTypesEntity> SetupDefaultTable(ISession session)
        {
            // drop table if exists, re-create
            var table = session.GetTable<ManyDataTypesEntity>();
            table.Create();

            List<ManyDataTypesEntity> allDataTypesRandomList = GetDefaultAllDataTypesList();
            //Insert some data
            foreach (var allDataTypesEntity in allDataTypesRandomList)
                table.Insert(allDataTypesEntity).Execute();

            return allDataTypesRandomList;
        }

        public static bool ListContains(List<ManyDataTypesEntity> expectedEntities, ManyDataTypesEntity actualEntity)
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

        public static void AssertListContains(List<ManyDataTypesEntity> expectedEntities, ManyDataTypesEntity actualEntity)
        {
            Assert.IsTrue(ListContains(expectedEntities, actualEntity));
        }

        public static void AssertListEqualsList(List<ManyDataTypesEntity> expectedEntities, List<ManyDataTypesEntity> actualEntities)
        {
            Assert.AreEqual(expectedEntities.Count, actualEntities.Count);
            foreach (var expectedEntity in expectedEntities)
                Assert.IsTrue(ListContains(actualEntities, expectedEntity));
        }


    }
}