using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Configuration;

namespace Cassandra.Data
{
    public class CqlProviderFactory : DbProviderFactory
    {
        public static readonly CqlProviderFactory Instance = new CqlProviderFactory();

        public CqlProviderFactory()
        {
        }

        public virtual CqlProviderFactory GetInstance()
        {
            return Instance;
        }

        public override bool CanCreateDataSourceEnumerator
        {
            get
            {
                return false;
            }
        }

        public override DbCommand CreateCommand()
        {
            return new CqlCommand();
        }

        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new CqlCommandBuilder();
        }

        public override DbConnection CreateConnection()
        {
            return new CqlConnection();
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new CassandraConnectionStringBuilder();
        }

        public override DbDataAdapter CreateDataAdapter()
        {
            return new CqlDataAdapter();
        }

        public override DbDataSourceEnumerator CreateDataSourceEnumerator()
        {
            throw new NotSupportedException();
        }

        public override DbParameter CreateParameter()
        {
            throw new NotSupportedException();
        }

        public override System.Security.CodeAccessPermission CreatePermission(System.Security.Permissions.PermissionState state)
        {
            throw new NotSupportedException();
        }
    }
}
