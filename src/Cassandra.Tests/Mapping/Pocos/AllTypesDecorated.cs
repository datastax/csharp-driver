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
using Cassandra.Data.Linq;

#pragma warning disable 618

namespace Cassandra.Tests.Mapping.Pocos
{
    /// <summary>
    /// Test utility: Represents an application entity with most of single types as properties
    /// </summary>
    [Table("atd")]
    public class AllTypesDecorated
    {
        [Column("boolean_VALUE")]
        public bool BooleanValue { get; set; }
        [Column("datetime_VALUE")]
        public DateTime DateTimeValue { get; set; }
        [Column("decimal_VALUE")]
        public decimal DecimalValue { get; set; }
        [Column("double_VALUE")]
        public double DoubleValue { get; set; }
        [Column("int64_VALUE")]
        public Int64 Int64Value { get; set; }
        [Column("int_VALUE")]
        public int IntValue { get; set; }
        [Column("string_VALUE")]
        public string StringValue { get; set; }
        [ClusteringKey(0)]
        [Column("timeuuid_VALUE")]
        public TimeUuid TimeUuidValue { get; set; }
        [PartitionKey]
        [Column("uuid_VALUE")]
        public Guid UuidValue { get; set; }
    }
}
