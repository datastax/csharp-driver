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
using Dse.Data.Linq;
#pragma warning disable 618

namespace Dse.Test.Unit.Mapping.Pocos
{
    [AllowFiltering]
    [Table("x_t")]
    public class LinqDecoratedEntity
    {
        [PartitionKey]
        [Column("x_pk")]
        public string pk { get; set; }

        [ClusteringKey(1)]
        [Column("x_ck1")]
        public int? ck1 { get; set; }

        [ClusteringKey(2)]
        [Column("x_ck2")]
        public int ck2 { get; set; }

        [Column("x_f1")]
        public int f1 { get; set; }

        [Ignore]
        public string Ignored1 { get; set; }

        [Ignore]
        public FluentUser Ignored2 { get; set; }
    }
}
