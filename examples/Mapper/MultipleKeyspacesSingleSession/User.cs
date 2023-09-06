//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using Cassandra.Mapping;

namespace MultipleKeyspacesSingleSession
{
    public class User
    {
        public string Name { get; set; }

        public Guid Id { get; set; }

        public static Map<User> GetUserMappingConfig(string keyspace)
        {
            return new Map<User>().KeyspaceName(keyspace).TableName("users")
                .PartitionKey(x => x.Id)
                .ClusteringKey(x => x.Id)
                .Column(x => x.Id, x => x.WithName("id"))
                .Column(x => x.Name, x => x.WithName("name"));
        }
    }
}
