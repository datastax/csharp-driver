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

    public class CqlContext : IDisposable
    {
        internal CassandraSession ManagedConnection = null;

        bool releaseOnClose;
        CqlKeyspace keyspace;
        string keyspaceName;
        CqlConsistencyLevel clRead;
        CqlConsistencyLevel clWrite;

        public CqlContext(CassandraSession cqlConnection, bool releaseOnClose = false, string keyspaceName = null)
        {
            this.ManagedConnection = cqlConnection;
            this.releaseOnClose = releaseOnClose;
            this.keyspaceName = keyspaceName;
            if (this.keyspaceName == null)
                this.keyspaceName = this.GetType().Name;

            this.keyspace = new CqlKeyspace(this, this.keyspaceName);
        }

        public CqlContext(string connectionString, string keyspaceName = null)
        {
            var cqlConnectionParams = new CqlConnectionStringBuilder(connectionString);
            this.releaseOnClose = true;

            this.keyspaceName = keyspaceName;
            if (this.keyspaceName == null)
                this.keyspaceName = cqlConnectionParams.Keyspace;

            if (this.keyspaceName == null)
                this.keyspaceName = this.GetType().Name;

            this.clRead = cqlConnectionParams.ReadCqlConsistencyLevel;
            this.clWrite = cqlConnectionParams.WriteCqlConsistencyLevel;

            ManagedConnection = new CassandraSession(
                cqlConnectionParams.ClusterEndpoints, keyspaceName, cqlConnectionParams.CompressionType, cqlConnectionParams.ConnectionTimeout, new CredentialsDelegate(getCredentials), cqlConnectionParams.MaxPoolSize);

            this.keyspace = new CqlKeyspace(this, this.keyspaceName);
        }

        private Dictionary<string, string> getCredentials(string auth)
        {
            return null;
        }

        public void Dispose()
        {
            if (releaseOnClose)
                ManagedConnection.Dispose();
        }

        CqlKeyspace Keyspace { get { return keyspace; } }

        Dictionary<string, ICqlTable> tables = new Dictionary<string, ICqlTable>();


        public void CreateKeyspaceIfNotExists(string ksname)
        {
            var keyspace = new CqlKeyspace(this, ksname);
            keyspace.CreateIfNotExists();
        }

        public void DeleteKeyspaceIfExists(string ksname)
        {
            var keyspace = new CqlKeyspace(this, ksname);
            keyspace.Delete();
        }

        public void CreateTablesIfNotExist()
        {
            foreach (var table in tables)
            {
                try
                {
                    CreateTable(table.Value, table.Key);
                }
                catch (Cassandra.CassandraClusterInvalidException)
                {
                    //already exists
                }
            }
        }


        internal void CreateTable(ICqlTable table, string tableName = null)
        {
            ExecuteNonQuery(CqlQueryTools.GetCreateCQL(table, tableName));
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

        internal void CreateKeyspace(string keyspace)
        {
            ManagedConnection.NonQuery(CqlQueryTools.GetCreateKeyspaceCQL(keyspace), CqlConsistencyLevel.IGNORE); //no need for Consistency lvl ~ Krzysiek 
        }

        internal CqlRowSet ExecuteRows(string cqlQuery)
        {
            //Keyspace.Select();
            return ManagedConnection.Query(cqlQuery, this.clRead);
        }

        internal void ExecuteNonQuery(string cqlQuery)
        {
            //Keyspace.Select();            
            ManagedConnection.NonQuery(cqlQuery, this.clWrite);
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
