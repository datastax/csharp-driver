//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Data.Common;

namespace Dse.Data
{
    /// <summary>
    /// Represents a set of methods for creating instances of a CQL ADO.NET implementation
    /// of the data source classes.
    /// </summary>
    public class CqlProviderFactory : DbProviderFactory
    {
        public static readonly CqlProviderFactory Instance = new CqlProviderFactory();

        public virtual CqlProviderFactory GetInstance()
        {
            return Instance;
        }

        public override DbCommand CreateCommand()
        {
            return new CqlCommand();
        }

        public override DbConnection CreateConnection()
        {
            return new CqlConnection();
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new CassandraConnectionStringBuilder();
        }

        public override DbParameter CreateParameter()
        {
            throw new NotSupportedException();
        }

#if NET45
        public override bool CanCreateDataSourceEnumerator
        {
            get { return false; }
        }

        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new CqlCommandBuilder();
        }

        public override DbDataAdapter CreateDataAdapter()
        {
            return new CqlDataAdapter();
        }

        public override DbDataSourceEnumerator CreateDataSourceEnumerator()
        {
            throw new NotSupportedException();
        }

        public override System.Security.CodeAccessPermission CreatePermission(System.Security.Permissions.PermissionState state)
        {
            throw new NotSupportedException();
        }
#endif
    }
}