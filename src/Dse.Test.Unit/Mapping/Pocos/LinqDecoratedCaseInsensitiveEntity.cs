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
    [Table(CaseSensitive=false, Name="tbl1")]
    public class LinqDecoratedCaseInsensitiveEntity
    {
        [Column("i_id"), PartitionKey]
        public long LongValue { get; set; }

        [Column("val1")]
        public string StringValue { get; set; }

        [Column("val2")]
        [SecondaryIndex]
        public string AnotherStringValue { get; set; }

        public DateTimeOffset Date { get; set; }
    }
}
