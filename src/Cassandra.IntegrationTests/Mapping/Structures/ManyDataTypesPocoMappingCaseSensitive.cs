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
