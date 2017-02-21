using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class CollectionTypesEntity
    {
        public long Id { get; set; }
        public List<int> Scores { get; set; }
        public string[] Tags { get; set; }
        public Dictionary<string, string> Favs { get; set; }
    }
}
