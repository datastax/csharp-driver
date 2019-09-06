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
    /// <summary>
    /// Note: the Table Name meta value here is lowercase so it will be compatible with default behavior with CqlPoco, please leave it lowercase.
    /// </summary>
    [AllowFiltering]
    [Table("alldatatypesnocolumnmeta")]
    public class AllDataTypesNoColumnMeta : IAllDataTypesEntity
    {
        [PartitionKey]
        public string StringType { get; set; }

        [ClusteringKey(1)]
        public Guid GuidType { get; set; }

        public DateTime DateTimeType { get; set; }

        public DateTime? NullableDateTimeType { get; set; }

        public DateTimeOffset DateTimeOffsetType { get; set; }

        public bool BooleanType { get; set; }

        public Decimal DecimalType { get; set; }

        public double DoubleType { get; set; }

        public float FloatType { get; set; }

        public int? NullableIntType { get; set; }

        public int IntType { get; set; }

        public Int64 Int64Type { get; set; }

        public TimeUuid TimeUuidType { get; set; }

        public TimeUuid? NullableTimeUuidType { get; set; }

        public Dictionary<string, long> DictionaryStringLongType { get; set; }

        public Dictionary<string, string> DictionaryStringStringType { get; set; }

        public List<Guid> ListOfGuidsType { get; set; }

        public List<string> ListOfStringsType { get; set; }

        public static AllDataTypesNoColumnMeta GetRandomInstance()
        {
            AllDataTypesNoColumnMeta adtncm = new AllDataTypesNoColumnMeta();
            return (AllDataTypesNoColumnMeta)AllDataTypesEntityUtil.Randomize(adtncm);
        }

        public void AssertEquals(AllDataTypesNoColumnMeta actualRow)
        {
            AllDataTypesEntityUtil.AssertEquals(this, actualRow);
        }

    }
}