using Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Linq.Structures
{
    public class CassandraLogContext : Context
    {
        public CassandraLogContext(ISession session)
            : base(session)
        {
            AddTable<CassandraLog>();
            CreateTablesIfNotExist();
        }
    }
}