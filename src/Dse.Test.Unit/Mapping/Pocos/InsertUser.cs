//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using Dse.Mapping;
using Dse.Mapping.Attributes;

namespace Dse.Test.Unit.Mapping.Pocos
{
    [Table("users")]
    public class InsertUser
    {
        // The main basic data types as properties
        [Column("userid")]
        public Guid Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTimeOffset CreatedDate { get; set; }
        public bool IsActive { get; set; }

        // Nullable types
        public DateTimeOffset? LastLoginDate { get; set; }

        // Collection types
        public List<DateTimeOffset> LoginHistory { get; set; }
        public HashSet<int> LuckyNumbers { get; set; }
        public Dictionary<string, int> ChildrenAges { get; set; }

        // Enum and nullable enum properties
        [Column(Type = typeof(string))]
        public RainbowColor FavoriteColor { get; set; }

        [Column(Type = typeof(string))]
        public UserType? TypeOfUser { get; set; }

        [Column("preferredcontactmethod", Type = typeof(int))]
        public ContactMethod PreferredContact { get; set; }

        [Column(Type = typeof(int?))]
        public HairColor? HairColor { get; set; }

        // A column that should be ignored
        [Ignore]
        public string SomeIgnoredProperty { get; set; }
    }
}
