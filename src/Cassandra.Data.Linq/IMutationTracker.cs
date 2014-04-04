using System.Collections.Generic;
using System.Text;

namespace Cassandra.Data.Linq
{
    public interface IMutationTracker
    {
        void SaveChangesOneByOne(Context context, string tablename, ConsistencyLevel consistencyLevel);
        bool AppendChangesToBatch(BatchStatement batchScript, string tablename);
        bool AppendChangesToBatch(StringBuilder batchScript, string tablename);
        void BatchCompleted(QueryTrace trace);
        List<QueryTrace> RetriveAllQueryTraces();
    }
}