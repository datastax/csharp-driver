using Cassandra.Data.Linq;

namespace Cassandra.Data.Linq.MSTest
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
