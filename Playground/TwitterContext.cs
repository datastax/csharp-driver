using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Cassandra.Data;
using Cassandra.Native;

namespace Playground
{
    public class TwitterContext : Context
    {
        public TwitterContext(Session session, ConsistencyLevel ReadCqlConsistencyLevel, ConsistencyLevel WriteCqlConsistencyLevel)
            : base(session, ReadCqlConsistencyLevel, WriteCqlConsistencyLevel)
        {
            AddTable<Tweet>();
            AddTable<Followers>();
            AddTable<FollowedTweet>();
            AddTable<Statistics>();
            CreateTablesIfNotExist();
        }
    }
}
