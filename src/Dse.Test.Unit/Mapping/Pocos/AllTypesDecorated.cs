using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
#pragma warning disable 618

namespace Cassandra.Tests.Mapping.Pocos
{
    /// <summary>
    /// Test utility: Represents an application entity with most of single types as properties
    /// </summary>
    [Table("atd")]
    public class AllTypesDecorated
    {
        [Column("boolean_VALUE")]
        public bool BooleanValue { get; set; }
        [Column("datetime_VALUE")]
        public DateTime DateTimeValue { get; set; }
        [Column("decimal_VALUE")]
        public decimal DecimalValue { get; set; }
        [Column("double_VALUE")]
        public double DoubleValue { get; set; }
        [Column("int64_VALUE")]
        public Int64 Int64Value { get; set; }
        [Column("int_VALUE")]
        public int IntValue { get; set; }
        [Column("string_VALUE")]
        public string StringValue { get; set; }
        [ClusteringKey(0)]
        [Column("timeuuid_VALUE")]
        public TimeUuid TimeUuidValue { get; set; }
        [PartitionKey]
        [Column("uuid_VALUE")]
        public Guid UuidValue { get; set; }
    }
}
