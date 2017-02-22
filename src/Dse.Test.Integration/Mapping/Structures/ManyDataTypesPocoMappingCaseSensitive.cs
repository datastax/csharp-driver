//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;

namespace Cassandra.IntegrationTests.Mapping.Structures
{
    class ManyDataTypesPocoMappingCaseSensitive : Map<ManyDataTypesPoco>
    {
        public ManyDataTypesPocoMappingCaseSensitive()
        {
            TableName("ManyDataTypesPoco");
            PartitionKey(u => u.StringType);
            Column(u => u.BooleanType).CaseSensitive();
            Column(u => u.DateTimeOffsetType).CaseSensitive();
            Column(u => u.DateTimeType).CaseSensitive();
            Column(u => u.DecimalType).CaseSensitive();
            Column(u => u.DictionaryStringLongType).CaseSensitive();
            Column(u => u.DictionaryStringStringType).CaseSensitive();
            Column(u => u.DoubleType).CaseSensitive();
            Column(u => u.FloatType).CaseSensitive();
            Column(u => u.GuidType).CaseSensitive();
            Column(u => u.Int64Type).CaseSensitive();
            Column(u => u.IntType).CaseSensitive();
            Column(u => u.ListOfGuidsType).CaseSensitive();
            Column(u => u.ListOfStringsType).CaseSensitive();
            Column(u => u.NullableIntType).CaseSensitive();
            Column(u => u.NullableTimeUuidType).CaseSensitive();
            Column(u => u.StringType).CaseSensitive();
            Column(u => u.TimeUuidType).CaseSensitive();

        }
    }
}
