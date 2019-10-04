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
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;

namespace Cassandra.Tests.Mapping.FluentMappings
{
    /// <summary>
    /// Defines how to map the FluentUser class.
    /// </summary>
    public class FluentUserMapping : Map<FluentUser>
    {
        public FluentUserMapping()
        {
            TableName("users");
            PartitionKey(u => u.Id);
            Column(u => u.Id, cm => cm.WithName("userid"));
            Column(u => u.FavoriteColor, cm => cm.WithDbType<string>());
            Column(u => u.TypeOfUser, cm => cm.WithDbType(typeof (string)));
            Column(u => u.PreferredContact, cm => cm.WithName("preferredcontactmethod").WithDbType<int>());
            Column(u => u.HairColor, cm => cm.WithDbType(typeof (int?)));
            Column(u => u.SomeIgnoredProperty, cm => cm.Ignore());
        }
    }
}
