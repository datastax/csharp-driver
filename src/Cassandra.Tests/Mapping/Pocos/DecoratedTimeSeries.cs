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
using Cassandra.Mapping.Attributes;

namespace Cassandra.Tests.Mapping.Pocos
{
    [Table(Name = "tbl1", Keyspace = "ks1", CaseSensitive = true)]
    public class DecoratedTimeSeries
    {
        [PartitionKey(0)]
        [Column("name")]
        public string SensorName { get; set; }

        [PartitionKey(1)]
        public int Slice { get; set; }

        [ClusteringKey]
        public TimeUuid Time { get; set; }

        [Column("val")]
        public double Value { get; set; }

        public string Value2 { get; set; }

        [Ignore]
        public double CalculatedValue { get; set; }
    }
}
