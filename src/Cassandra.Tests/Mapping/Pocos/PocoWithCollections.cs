using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class PocoWithCollections<T>
    {
        public int Id { get; set; }

        public IEnumerable<T> IEnumerable { get; set; }

        public SortedSet<T> SortedSet { get; set; }

        public T[] Array { get; set; }

        public List<T> List { get; set; }

        public SortedDictionary<T, string> SortedDictionaryTKeyString { get; set; }
        public HashSet<T> HashSet { get; set; }

        public Dictionary<T, string> DictionaryTKeyString { get; set; }
    }
}
