//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping.Attributes;

#pragma warning disable 618

namespace Cassandra.Tests.Mapping.Pocos
{
    [Table("x_ts", CaseSensitive = true)]
    public class LinqDecoratedWithStringCkEntity
    {
        [PartitionKey]
        [Column("x_pk")]
        public string pk { get; set; }

        [ClusteringKey(1)]
        [Column("x_ck1")]
        public string ck1 { get; set; }

        [Column("x_f1")]
        public int f1 { get; set; }
    }
}
