using System;
using System.Collections.Generic;
using CqlPoco.IntegrationTests.FluentMappings;

namespace CqlPoco.IntegrationTests.Pocos
{
    /// <summary>
    /// A user class that should have fluent mappings defined in the <see cref="FluentUserMapping"/> class.
    /// </summary>
    public class FluentUser
    {
        // The main basic data types as properties
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
        public RainbowColor FavoriteColor { get; set; }
        public UserType? TypeOfUser { get; set; }
        public ContactMethod PreferredContact { get; set; }
        public HairColor? HairColor { get; set; }

        // A column that should be ignored
        public string SomeIgnoredProperty { get; set; }
    }
}
