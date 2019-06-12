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
using Cassandra.Mapping;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class PocoWithEnumCollections
    {
        public long Id { get; set; }

        public HairColor Enum1 { get; set; }

        public List<HairColor> List1 { get; set; }

        public IList<HairColor> List2 { get; set; }
        
        public HairColor[] Array1 { get; set; }

        public SortedSet<HairColor> Set1 { get; set; }

        public ISet<HairColor> Set2 { get; set; }

        public HashSet<HairColor> Set3 { get; set; }

        public Dictionary<HairColor, TimeUuid> Dictionary1 { get; set; }

        public IDictionary<HairColor, TimeUuid> Dictionary2 { get; set; }
        
        public SortedDictionary<HairColor, TimeUuid> Dictionary3 { get; set; }

        public static Map<PocoWithEnumCollections> DefaultMapping => new Map<PocoWithEnumCollections>()
            .ExplicitColumns()
            .Column(x => x.Id, cm => cm.WithName("id"))
            .Column(x => x.List1, cm => cm.WithName("list1").WithDbType<IEnumerable<int>>())
            .Column(x => x.List2, cm => cm.WithName("list2").WithDbType<IEnumerable<int>>())
            .Column(x => x.Array1, cm => cm.WithName("array1").WithDbType<IEnumerable<int>>())
            .Column(x => x.Set1, cm => cm.WithName("set1").WithDbType<IEnumerable<int>>())
            .Column(x => x.Set2, cm => cm.WithName("set2").WithDbType<IEnumerable<int>>())
            .Column(x => x.Set3, cm => cm.WithName("set3").WithDbType<IEnumerable<int>>())
            .Column(x => x.Dictionary1, cm => cm.WithName("map1").WithDbType<IDictionary<int, Guid>>())
            .Column(x => x.Dictionary2, cm => cm.WithName("map2").WithDbType<IDictionary<int, Guid>>())
            .Column(x => x.Dictionary3, cm => cm.WithName("map3").WithDbType<IDictionary<int, Guid>>())
            .TableName("tbl1")
            .PartitionKey(x => x.Id);

        public static CqlColumn[] DefaultColumns => new[]
        {
            new CqlColumn {Name = "id", Index = 0, Type = typeof(long)},
            new CqlColumn {Name = "list1", Index = 1, Type = typeof(IEnumerable<int>)},
            new CqlColumn {Name = "list2", Index = 2, Type = typeof(IEnumerable<int>)},
            new CqlColumn {Name = "array1", Index = 3, Type = typeof(IEnumerable<int>)},
            new CqlColumn {Name = "set1", Index = 4, Type = typeof(IEnumerable<int>)},
            new CqlColumn {Name = "set2", Index = 5, Type = typeof(IEnumerable<int>)},
            new CqlColumn {Name = "set3", Index = 6, Type = typeof(IEnumerable<int>)},
            new CqlColumn {Name = "map1", Index = 7, Type = typeof(IDictionary<int, Guid>)},
            new CqlColumn {Name = "map2", Index = 8, Type = typeof(IDictionary<int, Guid>)},
            new CqlColumn {Name = "map3", Index = 9, Type = typeof(IDictionary<int, Guid>)}
        };

        public static string DefaultCreateTableCql = "CREATE TABLE {0} (id bigint PRIMARY KEY, list1 list<int>" +
                                                     ", list2 list<int>, array1 list<int>, set1 set<int>" +
                                                     ", set2 set<int>, set3 set<int>, map1 map<int, uuid>" +
                                                     ", map2 map<int, uuid>, map3 map<int, uuid>)";
    }
}