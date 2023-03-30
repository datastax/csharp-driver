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

using Linq = Cassandra.Data.Linq;
using Mapping = Cassandra.Mapping;

#pragma warning disable 618
namespace Cassandra.IntegrationTests.Linq.Structures
{
    [Linq::Table("test_map_empty_clust_column_name")]
    [Mapping::Attributes.Table("test_map_empty_clust_column_name")]
    // ReSharper disable once ClassNeverInstantiated.Local
    class EmptyClusteringColumnName
    {
        [Linq::PartitionKey]
        [Linq::Column("id")]
        [Mapping::Attributes.PartitionKey]
        [Mapping::Attributes.Column("id")]
        // ReSharper disable once UnusedMember.Local
        public int Id { get; set; }

        [Linq::ClusteringKey(1)]
        [Mapping::Attributes.ClusteringKey(1)]
        [Mapping::Attributes.Column]
        // ReSharper disable once InconsistentNaming
        // ReSharper disable once UnusedMember.Local
        public string cluster { get; set; }
            
        [Mapping::Attributes.Column]
        // ReSharper disable once InconsistentNaming
        // ReSharper disable once UnusedMember.Local
        public string value { get; set; }
    }
}
#pragma warning restore 618
