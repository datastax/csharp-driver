//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
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