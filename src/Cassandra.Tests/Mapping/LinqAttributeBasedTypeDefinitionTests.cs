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
using NUnit.Framework;
using SortOrder = Cassandra.Mapping.SortOrder;

#pragma warning disable 612
#pragma warning disable 618

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class LinqAttributeBasedColumnDefinitionTests
    {
        [Test]
        public void AttributeBased_Without_Name_For_Clustering_Key_Test()
        {
            var definition = new LinqAttributeBasedTypeDefinition(typeof(SamplePocoWithoutClusteringKeyName), "t", "k");
            CollectionAssert.AreEqual(new [] {Tuple.Create("Id2", SortOrder.Ascending)}, definition.ClusteringKeys);
            CollectionAssert.AreEqual(new[] { "Id1" }, definition.PartitionKeys);
        }

        private class SamplePocoWithoutClusteringKeyName
        {
            [PartitionKey]
            public int Id1 { get; set; }

            [ClusteringKey(0, SortOrder.Ascending)]
            [Column]
            public int Id2 { get; set; }
        }
    }
}