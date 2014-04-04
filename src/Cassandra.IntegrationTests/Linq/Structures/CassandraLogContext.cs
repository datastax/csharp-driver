using Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Linq.Structures
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