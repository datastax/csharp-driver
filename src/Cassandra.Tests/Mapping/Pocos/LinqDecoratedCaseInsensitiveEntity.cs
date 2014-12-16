using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;

namespace Cassandra.Tests.Mapping.Pocos
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
