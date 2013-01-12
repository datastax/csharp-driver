using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Cassandra.Data;

namespace Playground
{
    public class Statistics
    {        
        [PartitionKey]
        public string author_id;

        [Counter]
        public long tweets_count;
        
        [Counter]
        public long followers_count;
    }    
}
