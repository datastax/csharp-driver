using Cassandra;
using Cassandra.Data.Linq;

namespace Playground
{
    public class CassandraLogContext : Context
    {
        public CassandraLogContext(Session session)
            : base(session)
        {
            AddTable<CassandraLog>();
            CreateTablesIfNotExist();
        }
    }
}