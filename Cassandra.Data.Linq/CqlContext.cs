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
        public PartitionKeyAttribute(int Index=0) { this.Index = Index; }
        public int Index = -1;
    }

    public class ClusteringKeyAttribute : Attribute
    {
        public ClusteringKeyAttribute(int Index) { this.Index = Index; }
        public int Index = -1;
    }

    public class SecondaryIndexAttribute : Attribute
    {
    }

    public class CounterAttribute : Attribute
    {
    }

    public class CqlContext : IDisposable
    {
        internal CassandraSession ManagedConnection = null;

        CqlConsistencyLevel ReadCqlConsistencyLevel;
        CqlConsistencyLevel WriteCqlConsistencyLevel;

        bool releaseOnClose;
        string keyspaceName;
        public string Keyspace { get { return keyspaceName; } }

        void Initialize(CassandraSession cqlConnection, CqlConsistencyLevel ReadCqlConsistencyLevel, CqlConsistencyLevel WriteCqlConsistencyLevel, bool releaseOnClose)
        {
            this.ManagedConnection = cqlConnection;
            this.releaseOnClose = releaseOnClose;
            this.keyspaceName = cqlConnection.Keyspace;
            this.ReadCqlConsistencyLevel = ReadCqlConsistencyLevel;
            this.WriteCqlConsistencyLevel = WriteCqlConsistencyLevel;
        }
        public CqlContext(CassandraSession cqlSession, CqlConsistencyLevel ReadCqlConsistencyLevel, CqlConsistencyLevel WriteCqlConsistencyLevel, bool releaseOnClose = true)
        {
            Initialize(cqlSession, ReadCqlConsistencyLevel, WriteCqlConsistencyLevel, releaseOnClose);
        }

        public void Dispose()
        {
            if (releaseOnClose)
                if (ManagedConnection!=null)
                    ManagedConnection.Dispose();
        }

        Dictionary<string, ICqlTable> tables = new Dictionary<string, ICqlTable>();

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
                ExecuteWriteQuery(query);            
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

        internal CqlRowSet ExecuteReadQuery(string cqlQuery)
        {
            return ManagedConnection.Query(cqlQuery, ReadCqlConsistencyLevel);
        }

        internal void ExecuteWriteQuery(string cqlQuery)
        {
            var ret = ManagedConnection.Query(cqlQuery, WriteCqlConsistencyLevel);
            if (ret != null)
                throw new InvalidOperationException();
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
                StringBuilder counterBatchScript = new StringBuilder();
                foreach (var table in tables)
                    if(table.Value.isCounterTable)
                        table.Value.GetMutationTracker().AppendChangesToBatch(counterBatchScript, table.Key);
                    else
                        table.Value.GetMutationTracker().AppendChangesToBatch(batchScript, table.Key);
                                

                if (counterBatchScript.Length != 0)
                {
                    ExecuteWriteQuery("BEGIN COUNTER BATCH\r\n" + counterBatchScript.ToString() + "\r\nAPPLY BATCH");
                    foreach (var table in tables)
                        table.Value.GetMutationTracker().BatchCompleted();
                }
                
                if (batchScript.Length != 0)
                {
                    ExecuteWriteQuery("BEGIN BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY BATCH");
                    foreach (var table in tables)
                        table.Value.GetMutationTracker().BatchCompleted();
                }
            }
        }
    }
}
