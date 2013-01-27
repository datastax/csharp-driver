using Cassandra.Data.Linq;

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
