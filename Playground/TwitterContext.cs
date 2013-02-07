using Cassandra;
using Cassandra.Data.Linq;

namespace Playground
{
    public class TwitterContext : Context
    {
        public TwitterContext(Session session)
            : base(session)
        {
            AddTable<Tweet>();
            AddTable<Author>();
            AddTable<FollowedTweet>();
            AddTable<Statistics>();
            CreateTablesIfNotExist();
        }
    }
}
