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
        static Dictionary<string, CassandraManager> managedPools = new Dictionary<string, CassandraManager>();
        internal CassandraManagedConnection ManagedConnection = null;

        bool releaseOnClose;
        CqlKeyspace keyspace;
        string keyspaceName;

        public CqlContext(CassandraManagedConnection cqlConnection, bool releaseOnClose = false, string keyspaceName = null)
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
            lock (managedPools)
            {
                if (!managedPools.ContainsKey(cqlConnectionParams.PoolId))
                    managedPools.Add(cqlConnectionParams.PoolId, new CassandraManager(cqlConnectionParams.ClusterEndpoints, cqlConnectionParams.CompressionType, cqlConnectionParams.BufferingMode, cqlConnectionParams.ConnectionTimeout, new CredentialsDelegate(getCredentials), 1, cqlConnectionParams.MaxPoolSize));
                ManagedConnection = managedPools[cqlConnectionParams.PoolId].Connect();
            }

            this.keyspaceName = keyspaceName;
            if (this.keyspaceName == null)
                this.keyspaceName = cqlConnectionParams.Keyspace;

            if (this.keyspaceName == null)
                this.keyspaceName = this.GetType().Name;

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

        public CqlKeyspace Keyspace { get { return keyspace; } }

        Dictionary<string, ICqlTable> tables = new Dictionary<string, ICqlTable>();

        public void CreateTablesIfNotExist()
        {
            foreach (var table in tables)
            {
                try
                {
                    CreateTable(table.Value, table.Key);
                }
                catch (CassandraOutputException<OutputInvalid>)
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
            ProcessNonQuery(ManagedConnection.ExecuteQuery(CqlQueryTools.GetCreateKeyspaceCQL(keyspace)));
        }

        internal OutputRows ExecuteRows(string cqlQuery)
        {
            Keyspace.Select();
            return ProcessRows(ManagedConnection.ExecuteQuery(cqlQuery));
        }

        internal void ExecuteNonQuery(string cqlQuery)
        {
            Keyspace.Select();
            ProcessNonQuery(ManagedConnection.ExecuteQuery(cqlQuery));
        }

        internal object ExecuteScalar(string cqlQuery)
        {
            Keyspace.Select();
            return ProcessScallar(ManagedConnection.ExecuteQuery(cqlQuery));
        }

        private void ProcessNonQuery(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputVoid)
                    return;
                else
                    throw new InvalidOperationException();
            }
        }

        private object ProcessScallar(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputPrepared)
                    return (outp as OutputPrepared).QueryID;
                else if (outp is OutputSetKeyspace)
                    return (outp as OutputSetKeyspace).Value;
                else
                    throw new InvalidOperationException();
            }
        }

        private OutputRows ProcessRows(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputRows)
                    return (outp as OutputRows);
                else
                    throw new InvalidOperationException();
            }
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
