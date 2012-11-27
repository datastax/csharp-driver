using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Native;
using Cassandra.Data;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra.Data
{
    public class PartitionKeyAttribute : Attribute
    {
    }

    public class RowKeyAttribute : Attribute
    {
        public int Index = -1;
    }

    public class SecondaryIndexAttribute : Attribute
    {
    }

    public class CqlContext : IDisposable
    {
        internal CassandraSession ManagedConnection = null;

        CqlConnectionStringBuilder CqlConnectionStringBuilder = null;

        bool releaseOnClose;
        string keyspaceName;
        public string Keyspace { get { return keyspaceName; } }

        public CqlContext(CassandraSession cqlConnection, bool releaseOnClose = false)
        {
            this.ManagedConnection = cqlConnection;
            this.releaseOnClose = releaseOnClose;
            this.keyspaceName = cqlConnection.Keyspace;
        }

        public CqlContext(string connectionString, string keyspaceName = null, bool connect = true)
        {
            CqlConnectionStringBuilder = new CqlConnectionStringBuilder(connectionString);
            this.releaseOnClose = true;

            this.keyspaceName = keyspaceName;
            if (this.keyspaceName == null)
                this.keyspaceName = CqlConnectionStringBuilder.Keyspace;

            if (connect)
                Connect();
        }

        public void Connect()
        {
            if (ManagedConnection == null)
            {
                ManagedConnection = new CassandraSession(
                    CqlConnectionStringBuilder.ClusterEndpoints, this.keyspaceName, CqlConnectionStringBuilder.CompressionType, CqlConnectionStringBuilder.ConnectionTimeout, new CredentialsDelegate(getCredentials), CqlConnectionStringBuilder.MaxPoolSize);
            }
        }

        private Dictionary<string, string> getCredentials(string auth)
        {
            return null;
        }

        public void Dispose()
        {
            if (releaseOnClose)
                if (ManagedConnection!=null)
                    ManagedConnection.Dispose();
        }

        Dictionary<string, ICqlTable> tables = new Dictionary<string, ICqlTable>();

        public void CreateKeyspace(string ksname)
        {
            ManagedConnection.NonQuery(CqlQueryTools.GetCreateKeyspaceCQL(ksname), CqlConsistencyLevel.IGNORE); 
        }

        public void CreateKeyspaceIfNotExists(string ksname)
        {
            try
            {
                CreateKeyspace(ksname);
            }
            catch (CassandraClusterAlreadyExistsException)
            {
                //already exists
            }
        }

        public void DeleteKeyspace(string ksname)
        {
            ManagedConnection.NonQuery(CqlQueryTools.GetDropKeyspaceCQL(ksname), CqlConsistencyLevel.IGNORE);
        }
        public void DeleteKeyspaceIfExists(string ksname)
        {
            try
            {
                DeleteKeyspace(ksname);
            }
            catch (CassandraClusterConfigErrorException)
            {
                //not exists
            }
        }

        public void CreateTablesIfNotExist()
        {
            foreach (var table in tables)
            {
                try
                {
                    CreateTable(table.Value, table.Key);
                }
                catch (CassandraClusterAlreadyExistsException)
                {
                    //already exists
                }
            }
        }


        internal void CreateTable(ICqlTable table, string tableName = null)
        {
            var queries = CqlQueryTools.GetCreateCQL(table, tableName);                
            foreach(var query in queries)
                ExecuteNonQuery(query);            
        }

        public CqlTable<TEntity> AddTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof(TEntity).Name;
            var table = new CqlTable<TEntity>(this, tn);
            tables.Add(tn, table);
            return table;
        }

        public CqlTable<TEntity> AddTableIfNotHas<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof(TEntity).Name;
            if (!tables.ContainsKey(tn))
            {
                var table = new CqlTable<TEntity>(this, tn);
                tables.Add(tn, table);
                return table;
            }
            else
                return null;
        }
        
        public bool HasTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof(TEntity).Name;
            return tables.ContainsKey(tn);
        }

        public CqlTable<TEntity> GetTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof(TEntity).Name;
            return (CqlTable<TEntity>)tables[tn];
        }

        internal CqlRowSet ExecuteRows(string cqlQuery)
        {
            //Keyspace.Select();
            return ManagedConnection.Query(cqlQuery, CqlConnectionStringBuilder.ReadCqlConsistencyLevel);
        }

        internal void ExecuteNonQuery(string cqlQuery)
        {
            //Keyspace.Select();            
            ManagedConnection.NonQuery(cqlQuery, CqlConnectionStringBuilder.WriteCqlConsistencyLevel);
        }

        internal object ExecuteScalar(string cqlQuery)
        {
            //Keyspace.Select();
            return ManagedConnection.Scalar(cqlQuery);
        }

        public void SaveChanges(CqlSaveChangesMode mode = CqlSaveChangesMode.OneByOne)
        {
            if (mode == CqlSaveChangesMode.OneByOne)
            {
                foreach (var table in tables)
                    table.Value.GetMutationTracker().SaveChangesOneByOne(this, table.Key);
            }
            else
            {
                StringBuilder batchScript = new StringBuilder();
                batchScript.AppendLine("BEGIN BATCH");
                foreach (var table in tables)
                    table.Value.GetMutationTracker().AppendChangesToBatch(batchScript, table.Key);
                batchScript.AppendLine("APPLY BATCH");
                ExecuteNonQuery(batchScript.ToString());
                foreach (var table in tables)
                    table.Value.GetMutationTracker().BatchCompleted();
            }
        }

        
    }
}
