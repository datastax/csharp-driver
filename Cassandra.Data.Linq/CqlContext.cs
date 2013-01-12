using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Cassandra.Data;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra.Data
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class AllowFilteringAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class PartitionKeyAttribute : Attribute
    {
        public PartitionKeyAttribute(int Index = 0) { this.Index = Index; }
        public int Index = -1;
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        public ClusteringKeyAttribute(int Index) { this.Index = Index; }
        public int Index = -1;
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class SecondaryIndexAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class CounterAttribute : Attribute
    {
    }

    public class Context
    {
        internal Session ManagedSession = null;

        ConsistencyLevel ReadCqlConsistencyLevel;
        ConsistencyLevel WriteCqlConsistencyLevel;

        string keyspaceName;
        public string Keyspace { get { return keyspaceName; } }

        void Initialize(Session cqlConnection, ConsistencyLevel ReadCqlConsistencyLevel, ConsistencyLevel WriteCqlConsistencyLevel)
        {
            this.ManagedSession = cqlConnection;
            this.keyspaceName = cqlConnection.Keyspace;
            this.ReadCqlConsistencyLevel = ReadCqlConsistencyLevel;
            this.WriteCqlConsistencyLevel = WriteCqlConsistencyLevel;
        }
        public Context(Session cqlSession, ConsistencyLevel ReadCqlConsistencyLevel, ConsistencyLevel WriteCqlConsistencyLevel)
        {
            Initialize(cqlSession, ReadCqlConsistencyLevel, WriteCqlConsistencyLevel);
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
                catch (AlreadyExistsException)
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
            return ManagedSession.Execute(cqlQuery, ReadCqlConsistencyLevel);
        }

        internal void ExecuteWriteQuery(string cqlQuery)
        {
            var ret = ManagedSession.Execute(cqlQuery, WriteCqlConsistencyLevel);
            if (ret != null)
                throw new InvalidOperationException();
        }

        public void SaveChanges(SaveChangesMode mode = SaveChangesMode.OneByOne)
        {
            if (mode == SaveChangesMode.OneByOne)
            {
                foreach (var table in tables)
                    table.Value.GetMutationTracker().SaveChangesOneByOne(this, table.Key);
                foreach (var cplDels in additionalCommands)
                    cplDels.Execute();
            }
            else
            {
                StringBuilder batchScript = new StringBuilder();
                StringBuilder counterBatchScript = new StringBuilder();
                foreach (var table in tables)
                {
                    bool isCounter = false;
                    var props = table.Value.GetEntityType().GetPropertiesOrFields();
                    foreach (var prop in props)
                    {
                        Type tpy = prop.GetTypeFromPropertyOrField();
                        if (prop.GetCustomAttributes(typeof(CounterAttribute), true).FirstOrDefault() as CounterAttribute != null)
                        {
                            isCounter = true;
                            break;
                        }
                    }
                    if (isCounter)
                        table.Value.GetMutationTracker().AppendChangesToBatch(counterBatchScript, table.Key);
                    else
                        table.Value.GetMutationTracker().AppendChangesToBatch(batchScript, table.Key);

                }

                foreach (var cplDels in additionalCommands)
                    batchScript.AppendLine(cplDels.GetCql() + ";");
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
            additionalCommands.Clear();
        }

        List<ICqlCommand> additionalCommands = new List<ICqlCommand>();

        public void AppendCommand(ICqlCommand cqlCommand)
        {
            additionalCommands.Add(cqlCommand);
        }
    }
}
