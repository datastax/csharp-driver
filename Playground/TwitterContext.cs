using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Cassandra.Data;
using Cassandra.Native;

namespace Playground
{
    public class TwitterContext : CqlContext
    {
        public TwitterContext(CassandraSession session, CqlConsistencyLevel ReadCqlConsistencyLevel, CqlConsistencyLevel WriteCqlConsistencyLevel)
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
