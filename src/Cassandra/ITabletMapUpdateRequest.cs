// Copyright (C) DataStax Inc.
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

namespace Cassandra
{
    internal interface ITabletMapUpdateRequest
    {
        void Apply(TabletMap tabletMap);
    }

    internal class AddTabletRequest : ITabletMapUpdateRequest
    {
        private readonly string _keyspace;
        private readonly string _table;
        private readonly Tablet _tablet;

        public AddTabletRequest(string keyspace, string table, Tablet tablet)
        {
            _keyspace = keyspace;
            _table = table;
            _tablet = tablet;
        }

        public void Apply(TabletMap tabletMap)
        {
            tabletMap.AddTablet(_keyspace, _table, _tablet);
        }
    }

    internal class RemoveAllTabletsForKeyspace : ITabletMapUpdateRequest
    {
        private readonly string _keyspace;

        public RemoveAllTabletsForKeyspace(string keyspace)
        {
            _keyspace = keyspace;
        }

        public void Apply(TabletMap tabletMap)
        {
            tabletMap.RemoveTableMappings(_keyspace);
        }
    }

    internal class RemoveAllTabletsForTable : ITabletMapUpdateRequest
    {
        private readonly string _keyspace;
        private readonly string _table;

        public RemoveAllTabletsForTable(string keyspace, string table)
        {
            _keyspace = keyspace;
            _table = table;
        }

        public void Apply(TabletMap tabletMap)
        {
            tabletMap.RemoveTableMappings(_keyspace, _table);
        }
    }

    internal class RemoveAllTabletsForHost : ITabletMapUpdateRequest
    {
        private readonly Host _host;

        public RemoveAllTabletsForHost(Host h)
        {
            _host = h;
        }

        public void Apply(TabletMap tabletMap)
        {
            tabletMap.RemoveTableMappings(_host);
        }
    }
}
