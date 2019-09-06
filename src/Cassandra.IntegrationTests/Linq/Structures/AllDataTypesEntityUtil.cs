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
    [Table("allDataTypes")]
    public class AllDataTypesEntityUtil
    {
        public const int DefaultListLength = 5;

        public static IAllDataTypesEntity Randomize(IAllDataTypesEntity entity)
        {
            Dictionary<string, long> dictionaryStringLong = new Dictionary<string, long>() { { "key_" + Randomm.RandomAlphaNum(10), (long)1234321 } };
            Dictionary<string, string> dictionaryStringString = new Dictionary<string, string>() { { "key_" + Randomm.RandomAlphaNum(10), "value_" + Randomm.RandomAlphaNum(10) } };
            List<Guid> listOfGuidsType = new List<Guid>() { Guid.NewGuid(), Guid.NewGuid() };
            List<string> listOfStringsType = new List<string>() { Randomm.RandomAlphaNum(20), Randomm.RandomAlphaNum(12), "" };


            entity.StringType = "StringType_val_" + Randomm.RandomAlphaNum(10);
            entity.GuidType = Guid.NewGuid();
            entity.DateTimeType = DateTime.Now.ToUniversalTime();
            entity.NullableDateTimeType = Randomm.RandomInt() % 2 == 0 ? (DateTime?) null : DateTime.UtcNow;
            entity.DateTimeOffsetType = new DateTimeOffset();
            entity.BooleanType = false;
            entity.DecimalType = (decimal) 98765432.0;
            entity.DoubleType = (double) 9876543;
            entity.FloatType = (float) 987654;
            entity.NullableIntType = null;
            entity.IntType = 98765;
            entity.Int64Type = (Int64) 9876;
            entity.TimeUuidType = TimeUuid.NewId();
            entity.NullableTimeUuidType = null;
            entity.DictionaryStringLongType = dictionaryStringLong;
            entity.DictionaryStringStringType = dictionaryStringString;
            entity.ListOfGuidsType = listOfGuidsType;
            entity.ListOfStringsType = listOfStringsType;
            return entity;
        }

        public static void AssertEquals(IAllDataTypesEntity expectedEntity, IAllDataTypesEntity actualEntity)
        {
            Assert.AreEqual(expectedEntity.StringType, actualEntity.StringType);
            Assert.AreEqual(expectedEntity.GuidType, actualEntity.GuidType);
            Assert.AreEqual(expectedEntity.DateTimeType.ToString(), actualEntity.DateTimeType.ToString()); // We need 'ToString()' to round to the nearest second
            Assert.AreEqual(expectedEntity.DateTimeOffsetType.ToString(), actualEntity.DateTimeOffsetType.ToString());
            Assert.AreEqual(expectedEntity.BooleanType, actualEntity.BooleanType);
            Assert.AreEqual(expectedEntity.DecimalType, actualEntity.DecimalType);
            Assert.AreEqual(expectedEntity.DoubleType, actualEntity.DoubleType);
            Assert.AreEqual(expectedEntity.FloatType, actualEntity.FloatType);
            Assert.AreEqual(expectedEntity.IntType, actualEntity.IntType);
            Assert.AreEqual(expectedEntity.Int64Type, actualEntity.Int64Type);
            Assert.AreEqual(expectedEntity.TimeUuidType, actualEntity.TimeUuidType);
            Assert.AreEqual(expectedEntity.NullableTimeUuidType, actualEntity.NullableTimeUuidType);
            Assert.AreEqual(expectedEntity.DictionaryStringLongType, actualEntity.DictionaryStringLongType);
            Assert.AreEqual(expectedEntity.DictionaryStringStringType, actualEntity.DictionaryStringStringType);
            Assert.AreEqual(expectedEntity.ListOfGuidsType, actualEntity.ListOfGuidsType);
            Assert.AreEqual(expectedEntity.ListOfStringsType, actualEntity.ListOfStringsType);
        }

        public static bool AssertListContains(List<IAllDataTypesEntity> expectedEntities, IAllDataTypesEntity actualEntity)
        {
            foreach (var expectedEntity in expectedEntities)
            {
                try
                {
                    AssertEquals(expectedEntity, actualEntity);
                    return true;
                }
                catch (AssertionException) { }
            }
            return false;
        }

        public static bool AssertListContains(List<AllDataTypesEntity> expectedEntities, AllDataTypesEntity actualEntity)
        {
            foreach (var expectedEntity in expectedEntities)
            {
                try
                {
                    AssertEquals(expectedEntity, actualEntity);
                    return true;
                }
                catch (AssertionException) { }
            }
            return false;
        }

    }
}