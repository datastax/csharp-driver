using System;

namespace Cassandra.Data.Linq
{
    public interface ITable
    {
        void Create();
        Type GetEntityType();
        string GetQuotedTableName();
        ISession GetSession();
        TableType GetTableType();
    }
}