using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class PocoWithCollections
    {
        public int Id { get; set; }

        public IEnumerable<TimeUuid> IEnumerableTimeUuid { get; set; }

        public SortedSet<TimeUuid> SortedSetTimeUuid { get; set; }

        public TimeUuid[] ArrayTimeUuid { get; set; }

        public List<TimeUuid> ListTimeUuid { get; set; }

        public SortedDictionary<TimeUuid, string> SortedDictionaryTimeUuidString { get; set; }

        public Dictionary<TimeUuid, string> DictionaryTimeUuidString { get; set; }
    }
}
