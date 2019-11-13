//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;

namespace Cassandra.Tests.Mapping.Pocos
{
    /// <summary>
    /// A user decorated with attributes indicating how it should be mapped.
    /// </summary>
    [Table("users")]
    public class DecoratedUser
    {
        [Column("userid"), PartitionKey]
        public Guid Id { get; set; }

        public string Name { get; set; }
        public int Age { get; set; }

        [Ignore]
        public int? AnUnusedProperty { get; set; }
    }
}