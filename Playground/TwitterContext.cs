using Cassandra;
using Cassandra.Data.Linq;

namespace Playground
{
    public class TwitterContext : Context
    {
        public TwitterContext(Session session, ConsistencyLevel ReadCqlConsistencyLevel, ConsistencyLevel WriteCqlConsistencyLevel)
            : base(session, ReadCqlConsistencyLevel, WriteCqlConsistencyLevel)
        {
            AddTable<Tweet>();
            AddTable<Author>();
            AddTable<FollowedTweet>();
            AddTable<Statistics>();
            CreateTablesIfNotExist();
        }
    }
}
