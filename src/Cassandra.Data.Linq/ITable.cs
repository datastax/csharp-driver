using System;

namespace Cassandra.Data.Linq
{
    public interface ITable
    {
        void Create();
        Type GetEntityType();
        string GetQuotedTableName();
        Session GetSession();
        TableType GetTableType();
    }
}