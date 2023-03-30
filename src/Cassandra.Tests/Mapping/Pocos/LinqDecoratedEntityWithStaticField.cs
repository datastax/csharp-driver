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

using Cassandra.Data.Linq;
#pragma warning disable 618

namespace Cassandra.Tests.Mapping.Pocos
{
    [Table(Name = "Items", CaseSensitive = false)]
    public class LinqDecoratedEntityWithStaticField
    {
        [PartitionKey]
        public int Key { get; set; }

        [StaticColumn]
        public string KeyName { get; set; }

        [ClusteringKey(1)]
        public int ItemId { get; set; }

        public decimal Value { get; set; }
    }
}