using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Cassandra.Native;
using Cassandra.Data;

namespace Playground
{
    public class FollowedTweet
    {
        [PartitionKey]
        public string user_id;

        [RowKey]
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


    //public class FollowingTweetsContext : CqlContext
    //{
    //    public FollowingTweetsContext(CassandraSession session, CqlConsistencyLevel ReadCqlConsistencyLevel, CqlConsistencyLevel WriteCqlConsistencyLevel)
    //        :base(session,ReadCqlConsistencyLevel,WriteCqlConsistencyLevel)
    //    {
    //        AddTable<FollowedTweet>();
    //        CreateTablesIfNotExist();
    //    }

    //}
}
