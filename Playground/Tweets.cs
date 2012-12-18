using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data;
using Cassandra;
using Cassandra.Native;

namespace Playground
{
    public class Tweet
    {
        [PartitionKey]
        public string author_id;

        [ClusteringKey(0)]
        public Guid tweet_id;
        
        [SecondaryIndex]        
        public DateTimeOffset date;
                        
        public string body;  
      
        public void display()
        {
            Console.WriteLine("Author: " + this.author_id);
            Console.WriteLine("Date: " + this.date.ToString());
            Console.WriteLine("Tweet content: " + this.body + Environment.NewLine);
        }
    }           
}
