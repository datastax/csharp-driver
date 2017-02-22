//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using Dse.Data.Linq;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;
#pragma warning disable 618

namespace Dse.Test.Integration.Linq.Structures
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