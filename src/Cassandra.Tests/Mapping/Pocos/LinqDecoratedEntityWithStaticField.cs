﻿using Cassandra.Data.Linq;

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