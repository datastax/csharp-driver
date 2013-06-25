using System;
using Cassandra.Data.Linq;

namespace Cassandra.Data.Linq.MSTest
{
    public class FollowedTweet
    {
        [PartitionKey]
        public string user_id;

        [ClusteringKey(0)]
        public Guid tweet_id;
                
        [SecondaryIndex]
        public DateTimeOffset date;

        public string author_id;

        public string body;

        public void display()
        {
            Console.WriteLine("Author: " + this.author_id);
            Console.WriteLine("Date: " + this.date.ToString());
            Console.WriteLine("Tweet content: " + this.body + Environment.NewLine);
        }
    }
}
