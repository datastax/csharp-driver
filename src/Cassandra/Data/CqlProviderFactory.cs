//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Data.Common;

namespace Cassandra.Data
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

#if NETFRAMEWORK
        public override System.Security.CodeAccessPermission CreatePermission(System.Security.Permissions.PermissionState state)
        {
            throw new NotSupportedException();
        }
#endif
    }
}