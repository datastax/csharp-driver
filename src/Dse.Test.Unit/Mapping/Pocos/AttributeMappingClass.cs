//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Mapping;
using Dse.Mapping.Attributes;

namespace Dse.Test.Unit.Mapping.Pocos
{
    [Table("attr_mapping_class_table")]
    public class AttributeMappingClass
    {
        [PartitionKey]
        [Column("partition_key")]
        public int PartitionKey { get; set; }

        [ClusteringKey(0, SortOrder.Ascending, Name = "clustering_key_0")]
        public long ClusteringKey0 { get; set; }

        [ClusteringKey(1, SortOrder.Ascending, Name = "clustering_key_1")]
        public string ClusteringKey1 { get; set; }

        [ClusteringKey(2, SortOrder.Descending, Name = "clustering_key_2")]
        public Guid ClusteringKey2 { get; set; }

        [Column("bool_value_col")]
        public bool BoolValue { get; set; }

        [Column("float_value_col")]
        public float FloatValue { get; set; }

        [Column("decimal_value_col")]
        public decimal DecimalValue { get; set; }
        
        private static readonly IDictionary<string, Func<AttributeMappingClass, object>> ColumnMappings =
            new Dictionary<string, Func<AttributeMappingClass, object>>
            {
                { "bool_value_col", entity => entity.BoolValue },
                { "clustering_key_0", entity => entity.ClusteringKey0 },
                { "clustering_key_1", entity => entity.ClusteringKey1 },
                { "clustering_key_2", entity => entity.ClusteringKey2 },
                { "decimal_value_col", entity => entity.DecimalValue },
                { "float_value_col", entity => entity.FloatValue },
                { "partition_key", entity => entity.PartitionKey }
            };
        
        public object[] GetParameters()
        {
            return AttributeMappingClass.ColumnMappings.Values.Select(func => func(this)).ToArray();
        }
    }
}
