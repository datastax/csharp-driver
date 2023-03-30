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

using Cassandra.Mapping;

namespace Cassandra.IntegrationTests.Mapping.Structures
{
    /// <summary>
    /// Defines how to map the User class.
    /// </summary>
    public class UserMapping : Map<User>
    {
        public UserMapping()
        {
            TableName("users");
            PartitionKey(u => u.Id);
            Column(u => u.HairColor, cm => cm.WithDbType<string>()).CaseSensitive();
            Column(u => u.Id, cm => cm.WithName("Id"));
            Column(u => u.IsActive, cm => cm.WithName("IsActive"));
            Column(u => u.PreferredContactMethod, cm => cm.WithName("PreferredContactMethod").WithDbType<string>().WithName("PreferredContactMethod"));
            Column(u => u.SomeIgnoredProperty, cm => cm.Ignore());
            Column(u => u.TypeOfUser, cm => cm.WithDbType<int>().WithName("TypeOfUser"));
            Column(u => u.LastLoginDate, cm => cm.WithName("LastLoginDate"));
        }
    }
}
