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

namespace Cassandra.Tests.Mapping.Pocos
{
    /// <summary>
    /// Test utility: Represents an application entity with most of single types as properties
    /// </summary>
    public class AllTypesEntity
    {
        public bool BooleanValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public decimal DecimalValue { get; set; }
        public double DoubleValue { get; set; }
        public Int64 Int64Value { get; set; }
        public int IntValue { get; set; }
        public string StringValue { get; set; }
        public Guid UuidValue { get; set; }
    }
}
