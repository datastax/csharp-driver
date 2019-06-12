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
using System.Linq;
using Cassandra.Data.Linq;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.Structures
{
    [AllowFiltering]
    [Table("allDataTypes")]
    public class AllDataTypesEntity : IAllDataTypesEntity
    {
        public const int DefaultListLength = 5;

        [PartitionKey]
        [Column("string_type")]
        public string StringType { get; set; }

        [ClusteringKey(1)]
        [Column("guid_type")]
        public Guid GuidType { get; set; }

        [Column("date_time_type")]
        public DateTime DateTimeType { get; set; }

        [Column("nullable_date_time_type")]
        public DateTime? NullableDateTimeType { get; set; }

        [Column("date_time_offset_type")]
        public DateTimeOffset DateTimeOffsetType { get; set; }

        [Column("boolean_type")]
        public bool BooleanType { get; set; }

        [Column("decimal_type")]
        public Decimal DecimalType { get; set; }

        [Column("double_type")]
        public double DoubleType { get; set; }

        [Column("float_type")]
        public float FloatType { get; set; }

        [Column("nullable_int_type")]
        public int? NullableIntType { get; set; }

        [Column("int_type")]
        public int IntType { get; set; }

        [Column("int64_type")]
        public Int64 Int64Type { get; set; }

        [Column("time_uuid_type")]
        public TimeUuid TimeUuidType { get; set; }

        [Column("nullable_time_uuid_type")]
        public TimeUuid? NullableTimeUuidType { get; set; }

        [Column("map_type_string_long_type")]
        public Dictionary<string, long> DictionaryStringLongType { get; set; }

        [Column("map_type_string_string_type")]
        public Dictionary<string, string> DictionaryStringStringType { get; set; }

        [Column("list_of_guids_type")]
        public List<Guid> ListOfGuidsType { get; set; }

        [Column("list_of_strings_type")]
        public List<string> ListOfStringsType { get; set; }

        public static AllDataTypesEntity GetRandomInstance()
        {
            AllDataTypesEntity adte = new AllDataTypesEntity();
            return (AllDataTypesEntity)AllDataTypesEntityUtil.Randomize(adte);
        }

        public void AssertEquals(AllDataTypesEntity actualEntity)
        {
            AllDataTypesEntityUtil.AssertEquals(this, actualEntity);
        }

        public static List<AllDataTypesEntity> GetDefaultAllDataTypesList()
        {
            List<AllDataTypesEntity> objectList = new List<AllDataTypesEntity>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                objectList.Add(GetRandomInstance());
            }
            return objectList;
        }

        public static List<AllDataTypesEntity> SetupDefaultTable(ISession session)
        {
            // drop table if exists, re-create
            var table = new Table<AllDataTypesEntity>(session, new Cassandra.Mapping.MappingConfiguration());
            table.Create();

            List<AllDataTypesEntity> allDataTypesRandomList = GetDefaultAllDataTypesList();
            //Insert some data
            foreach (var allDataTypesEntity in allDataTypesRandomList)
                table.Insert(allDataTypesEntity).Execute();

            return allDataTypesRandomList;
        }

        public static AllDataTypesEntity WriteReadValidate(Table<AllDataTypesEntity> table)
        {
            WriteReadValidateUsingSessionBatch(table);
            return WriteReadValidateUsingTableMethods(table);
        }

        private static AllDataTypesEntity WriteReadValidateUsingTableMethods(Table<AllDataTypesEntity> table)
        {
            AllDataTypesEntity expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            string uniqueKey = expectedDataTypesEntityRow.StringType;

            // insert record
            table.GetSession().Execute(table.Insert(expectedDataTypesEntityRow));

            // select record
            List<AllDataTypesEntity> listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            AllDataTypesEntity actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
            return expectedDataTypesEntityRow;
        }

        private static AllDataTypesEntity WriteReadValidateUsingSessionBatch(Table<AllDataTypesEntity> table)
        {
            Batch batch = table.GetSession().CreateBatch();
            AllDataTypesEntity expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            string uniqueKey = expectedDataTypesEntityRow.StringType;
            batch.Append(table.Insert(expectedDataTypesEntityRow));
            batch.Execute();

            List<AllDataTypesEntity> listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            AllDataTypesEntity actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
            return expectedDataTypesEntityRow;
        }


    }
}