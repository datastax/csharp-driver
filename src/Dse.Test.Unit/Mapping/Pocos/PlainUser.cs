//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;

namespace Dse.Test.Unit.Mapping.Pocos
{
    /// <summary>
    /// A user POCO with no decorations.
    /// </summary>
    public class PlainUser
    {
        public PlainUser()
        {
            LoginHistory = new List<DateTimeOffset>();
            LuckyNumbers = new HashSet<int>();
            ChildrenAges = new SortedDictionary<string, int>();
        }
        // The main basic data types as properties
        public Guid UserId { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTimeOffset CreatedDate { get; set; }
        public bool IsActive { get; set; }

        // Nullable types
        public DateTimeOffset? LastLoginDate { get; set; }

        // Collection types
        public List<DateTimeOffset> LoginHistory { get; set; }
        public HashSet<int> LuckyNumbers { get; set; }
        public SortedDictionary<string, int> ChildrenAges { get; set; }

        // Enum and nullable enum properties
        public RainbowColor FavoriteColor { get; set; }
        public UserType? TypeOfUser { get; set; }
        public ContactMethod PreferredContactMethod { get; set; }
        public HairColor? HairColor { get; set; }
    }
}