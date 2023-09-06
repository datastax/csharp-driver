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

using System.Collections.Concurrent;
using Cassandra;
using Cassandra.Mapping;

namespace MultipleKeyspacesSingleSession
{
    public class MapperManager
    {
        private readonly ConcurrentDictionary<string, IMapper> _mappers = new ConcurrentDictionary<string, IMapper>();

        private ISession _session;

        public MapperManager(ISession session)
        {
            _session = session;
        }

        public IMapper GetMapperForKeyspace(string keyspace)
        {
            // ConcurrentDictionary.GetOrAdd second argument (valueFactory)
            // can be called multiple times for the same key but only 1 value will be added to the dictionary
            // 
            // In this case it is fine if multiple MapperManager objects are created and not used, they will just get garbage collected
            //
            // Source: Remarks section here https://learn.microsoft.com/en-us/dotnet/api/system.collections.concurrent.concurrentdictionary-2.getoradd?view=net-7.0
            return _mappers.GetOrAdd(keyspace, ks =>
            {
                var mapConfig = new MappingConfiguration();
                mapConfig.Define(User.GetUserMappingConfig(keyspace));
                // extra mapConfig.Define() calls for other entities here
                return new Mapper(_session, mapConfig);
            });
        }
    }
}
