using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class AllowFilteringAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class PartitionKeyAttribute : Attribute
    {
        public PartitionKeyAttribute(int index = 0) { this.Index = index; }
        public int Index = -1;
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        public ClusteringKeyAttribute(int index) { this.Index = index; }
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

        ConsistencyLevel _readCqlConsistencyLevel;
        ConsistencyLevel _writeCqlConsistencyLevel;

        string _keyspaceName;
        public string Keyspace { get { return _keyspaceName; } }

        void Initialize(Session cqlConnection, ConsistencyLevel readCqlConsistencyLevel, ConsistencyLevel writeCqlConsistencyLevel)
        {
            this.ManagedSession = cqlConnection;
            this._keyspaceName = cqlConnection.Keyspace;
            this._readCqlConsistencyLevel = readCqlConsistencyLevel;
            this._writeCqlConsistencyLevel = writeCqlConsistencyLevel;
        }
        public Context(Session cqlSession, ConsistencyLevel readCqlConsistencyLevel, ConsistencyLevel writeCqlConsistencyLevel)
        {
            Initialize(cqlSession, readCqlConsistencyLevel, writeCqlConsistencyLevel);
        }

        readonly Dictionary<string, ICqlTable> _tables = new Dictionary<string, ICqlTable>();

        public void CreateTablesIfNotExist()
        {
            foreach (var table in _tables)
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
            _tables.Add(tn, table);
            return table;
        }

        public CqlTable<TEntity> AddTableIfNotHas<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof(TEntity).Name;
            if (!_tables.ContainsKey(tn))
            {
                var table = new CqlTable<TEntity>(this, tn);
                _tables.Add(tn, table);
                return table;
            }
            else
                return null;
        }
        
        public bool HasTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof(TEntity).Name;
            return _tables.ContainsKey(tn);
        }

        public CqlTable<TEntity> GetTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof(TEntity).Name;
            return (CqlTable<TEntity>)_tables[tn];
        }

        internal CqlRowSet ExecuteReadQuery(string cqlQuery)
        {
            return ManagedSession.Execute(cqlQuery, _readCqlConsistencyLevel);
        }

        internal void ExecuteWriteQuery(string cqlQuery)
        {
            var ret = ManagedSession.Execute(cqlQuery, _writeCqlConsistencyLevel);
            if (ret != null)
                throw new InvalidOperationException();
        }

        public void SaveChanges(SaveChangesMode mode = SaveChangesMode.OneByOne)
        {
            if (mode == SaveChangesMode.OneByOne)
            {
                foreach (var table in _tables)
                    table.Value.GetMutationTracker().SaveChangesOneByOne(this, table.Key);
                foreach (var cplDels in _additionalCommands)
                    cplDels.Execute();
            }
            else
            {
                var batchScript = new StringBuilder();
                var counterBatchScript = new StringBuilder();
                foreach (var table in _tables)
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

                foreach (var cplDels in _additionalCommands)
                    batchScript.AppendLine(cplDels.GetCql() + ";");
                if (counterBatchScript.Length != 0)
                {
                    ExecuteWriteQuery("BEGIN COUNTER BATCH\r\n" + counterBatchScript.ToString() + "\r\nAPPLY BATCH");
                    foreach (var table in _tables)
                        table.Value.GetMutationTracker().BatchCompleted();
                }


                if (batchScript.Length != 0)
                {
                    ExecuteWriteQuery("BEGIN BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY BATCH");
                    foreach (var table in _tables)
                        table.Value.GetMutationTracker().BatchCompleted();
                }
            }
            _additionalCommands.Clear();
        }

        readonly List<ICqlCommand> _additionalCommands = new List<ICqlCommand>();

        public void AppendCommand(ICqlCommand cqlCommand)
        {
            _additionalCommands.Add(cqlCommand);
        }
    }
}
