//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;

namespace Cassandra.Tests.Mapping.Pocos
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