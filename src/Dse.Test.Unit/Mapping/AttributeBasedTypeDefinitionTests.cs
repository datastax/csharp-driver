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
using Dse.Mapping;
using Dse.Mapping.Attributes;
using Dse.Test.Unit.Mapping.Pocos;
using NUnit.Framework;

namespace Dse.Test.Unit.Mapping
{
    [TestFixture]
    public class AttributeBasedTypeDefinitionTests
    {
        [Test]
        public void AttributeBasedTypeDefinition_Defaults_Tests()
        {
            //Non decorated Poco
            var definition = new AttributeBasedTypeDefinition(typeof(AllTypesEntity));
            Assert.False(definition.CaseSensitive);
            Assert.False(definition.CompactStorage);
            Assert.False(definition.AllowFiltering);
            Assert.False(definition.ExplicitColumns);
            Assert.AreEqual(0, definition.ClusteringKeys.Length);
            Assert.AreEqual(0, definition.PartitionKeys.Length);
            Assert.Null(definition.KeyspaceName);
            Assert.AreEqual("AllTypesEntity", definition.TableName);
            Assert.AreEqual(typeof(AllTypesEntity), definition.PocoType);
        }

        [Test]
        public void AttributeBased_Single_PartitionKey_Test()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(DecoratedUser));
            Assert.False(definition.CaseSensitive);
            Assert.False(definition.CompactStorage);
            Assert.False(definition.AllowFiltering);
            Assert.False(definition.ExplicitColumns);
            Assert.AreEqual(0, definition.ClusteringKeys.Length);
            CollectionAssert.AreEqual(new[] { "userid" }, definition.PartitionKeys);
        }

        [Test]
        public void AttributeBased_Composite_PartitionKey_Test()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(DecoratedTimeSeries));
            Assert.True(definition.CaseSensitive);
            Assert.False(definition.CompactStorage);
            Assert.False(definition.AllowFiltering);
            Assert.False(definition.ExplicitColumns);
            CollectionAssert.AreEqual(new [] {Tuple.Create("Time", SortOrder.Unspecified)}, definition.ClusteringKeys);
            CollectionAssert.AreEqual(new[] { "name", "Slice" }, definition.PartitionKeys);
        }

        [Test]
        public void AttributeBased_Without_Name_For_Clustering_Key_Test()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(SamplePocoWithoutClusteringKeyName));
            Assert.False(definition.CaseSensitive);
            Assert.False(definition.CompactStorage);
            Assert.False(definition.AllowFiltering);
            Assert.False(definition.ExplicitColumns);
            CollectionAssert.AreEqual(new [] {Tuple.Create("Id2", SortOrder.Unspecified)}, definition.ClusteringKeys);
            CollectionAssert.AreEqual(new[] { "Id1" }, definition.PartitionKeys);
        }

        private class SamplePocoWithoutClusteringKeyName
        {
            [PartitionKey]
            public int Id1 { get; set; }

            [ClusteringKey]
            [Column]
            public int Id2 { get; set; }
        }
    }
}
