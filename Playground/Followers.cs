using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data;
using Cassandra;
using Cassandra.Native;

namespace Playground
{
    public class Followers
    {
        [PartitionKey]
        public string author_id;
        
        public List<string> followers;

        public void displayFollowers()
        {
            Console.WriteLine(this.author_id + " is followed by:" + Environment.NewLine);
            if (followers != null)
                foreach (var follower in this.followers)
                    Console.Write(follower + "  ");
            else
                Console.WriteLine("Nobody!");
        }
    }
    

    public class FollowersContext : CqlContext
    {
        public FollowersContext(CassandraSession session, CqlConsistencyLevel ReadCqlConsistencyLevel, CqlConsistencyLevel WriteCqlConsistencyLevel)
            :base(session,ReadCqlConsistencyLevel,WriteCqlConsistencyLevel)
        {
            AddTable<Followers>();
            CreateTablesIfNotExist();
        }


    }
}
