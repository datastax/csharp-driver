using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cassandra.Data.Linq
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

        private ConsistencyLevel _readCqlConsistencyLevel;
        private ConsistencyLevel _writeCqlConsistencyLevel;

        private string _keyspaceName;

        public string Keyspace
        {
            get { return _keyspaceName; }
        }

        private void Initialize(Session cqlConnection, ConsistencyLevel readCqlConsistencyLevel,
                                ConsistencyLevel writeCqlConsistencyLevel)
        {
            this.ManagedSession = cqlConnection;
            this._keyspaceName = cqlConnection.Keyspace;
            this._readCqlConsistencyLevel = readCqlConsistencyLevel;
            this._writeCqlConsistencyLevel = writeCqlConsistencyLevel;
        }

        public Context(Session cqlSession, ConsistencyLevel readCqlConsistencyLevel,
                       ConsistencyLevel writeCqlConsistencyLevel)
        {
            Initialize(cqlSession, readCqlConsistencyLevel, writeCqlConsistencyLevel);
        }

        private readonly Dictionary<string, ITable> _tables = new Dictionary<string, ITable>();

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


        internal void CreateTable(ITable table, string tableName = null)
        {
            var queries = CqlQueryTools.GetCreateCQL(table, tableName);
            foreach (var query in queries)
                ExecuteWriteQuery(query);
        }

        public Table<TEntity> AddTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof (TEntity).Name;
            var table = new Table<TEntity>(this, tn);
            _tables.Add(tn, table);
            return table;
        }

        public Table<TEntity> AddTableIfNotHas<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof (TEntity).Name;
            if (!_tables.ContainsKey(tn))
            {
                var table = new Table<TEntity>(this, tn);
                _tables.Add(tn, table);
                return table;
            }
            else
                return null;
        }

        public bool HasTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof (TEntity).Name;
            return _tables.ContainsKey(tn);
        }

        public Table<TEntity> GetTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof (TEntity).Name;
            return (Table<TEntity>) _tables[tn];
        }

        internal CqlRowSet ExecuteReadQuery(string cqlQuery)
        {
            return ManagedSession.Execute(cqlQuery, _readCqlConsistencyLevel);
        }

        internal IAsyncResult BeginExecuteReadQuery(string cqlQuery, AsyncCallback callback, object state, object tag)
        {
            return ManagedSession.BeginExecute(cqlQuery, callback, state, _readCqlConsistencyLevel, tag);
        }

        internal CqlRowSet EndExecuteReadQuery(IAsyncResult ar)
        {
            return ManagedSession.EndExecute(ar);
        }

        internal CqlRowSet ExecuteWriteQuery(string cqlQuery)
        {
            return ManagedSession.Execute(cqlQuery, _writeCqlConsistencyLevel);
        }

        internal IAsyncResult BeginExecuteWriteQuery(string cqlQuery, AsyncCallback callback, object state,
                                                     object tag)
        {
            return ManagedSession.BeginExecute(cqlQuery, callback, state, _writeCqlConsistencyLevel, tag);
        }

        internal CqlRowSet EndExecuteWriteQuery(IAsyncResult ar)
        {
            return ManagedSession.EndExecute(ar);
        }

        private struct CqlSaveTag
        {
            public Dictionary<string, TableType> TableTypes;
            public TableType TableType;
            public List<ICqlCommand> NewAdditionalCommands;
        }

        public IAsyncResult BeginSaveChangesBatch(TableType tableType, AsyncCallback callback, object state)
        {
            if (tableType == TableType.All)
                throw new ArgumentOutOfRangeException("tableType");

            var tableTypes = new Dictionary<string, TableType>();

            foreach (var table in _tables)
            {
                bool isCounter = false;
                var props = table.Value.GetEntityType().GetPropertiesOrFields();
                foreach (var prop in props)
                {
                    Type tpy = prop.GetTypeFromPropertyOrField();
                    if (
                        prop.GetCustomAttributes(typeof(CounterAttribute), true).FirstOrDefault() as
                        CounterAttribute != null)
                    {
                        isCounter = true;
                        break;
                    }
                }
                tableTypes.Add(table.Key, isCounter ? TableType.Standard : TableType.Counter);
            }

            var newAdditionalCommands = new List<ICqlCommand>();
            if (((tableType & TableType.Counter) == TableType.Counter))
            {
                var counterBatchScript = new StringBuilder();
                foreach (var table in _tables)
                    if (tableTypes[table.Key] == TableType.Counter)
                        table.Value.GetMutationTracker().AppendChangesToBatch(counterBatchScript, table.Key);

                foreach (var cplDels in _additionalCommands)
                    if (tableTypes[cplDels.GetTable().GetTableName()] == TableType.Counter)
                        counterBatchScript.AppendLine(cplDels.GetCql() + ";");
                    else if (tableType == TableType.Counter)
                        newAdditionalCommands.Add(cplDels);

                if (counterBatchScript.Length != 0)
                {
                    ExecuteWriteQuery("BEGIN COUNTER BATCH\r\n" + counterBatchScript.ToString() + "\r\nAPPLY BATCH");

                    return BeginExecuteWriteQuery(
                        "BEGIN COUNTER BATCH\r\n" + counterBatchScript.ToString() + "\r\nAPPLY BATCH", callback, state,
                        new CqlSaveTag() {TableTypes=tableTypes, TableType = TableType.Counter, NewAdditionalCommands = newAdditionalCommands });
                }
            }


            if (((tableType & TableType.Standard) == TableType.Standard))
            {
                var batchScript = new StringBuilder();
                foreach (var table in _tables)
                    if (tableTypes[table.Key] == TableType.Standard)
                        table.Value.GetMutationTracker().AppendChangesToBatch(batchScript, table.Key);

                foreach (var cplDels in _additionalCommands)
                    if (tableTypes[cplDels.GetTable().GetTableName()] == TableType.Standard)
                        batchScript.AppendLine(cplDels.GetCql() + ";");
                    else if (tableType == TableType.Standard)
                        newAdditionalCommands.Add(cplDels);

                if (batchScript.Length != 0)
                {
                    return BeginExecuteWriteQuery("BEGIN BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY BATCH", callback, state,
                        new CqlSaveTag() { TableTypes = tableTypes, TableType = TableType.Standard, NewAdditionalCommands = newAdditionalCommands });
                }
            }

            throw new InvalidOperationException();
        }

        public void EndSaveChangesBatch(IAsyncResult ar)
        {
            EndExecuteWriteQuery(ar);

            var tag = (CqlSaveTag) Session.GetTag(ar);
            foreach (var table in _tables)
                if (tag.TableTypes[table.Key] == tag.TableType)
                    table.Value.GetMutationTracker().BatchCompleted();
            _additionalCommands = tag.NewAdditionalCommands;
        }

        public void SaveChanges(SaveChangesMode mode = SaveChangesMode.Batch, TableType tableType = TableType.All)
        {
            var tableTypes = new Dictionary<string, TableType>();

            foreach (var table in _tables)
            {
                bool isCounter = false;
                var props = table.Value.GetEntityType().GetPropertiesOrFields();
                foreach (var prop in props)
                {
                    Type tpy = prop.GetTypeFromPropertyOrField();
                    if (
                        prop.GetCustomAttributes(typeof(CounterAttribute), true).FirstOrDefault() as
                        CounterAttribute != null)
                    {
                        isCounter = true;
                        break;
                    }
                }
                tableTypes.Add(table.Key, isCounter ? TableType.Counter : TableType.Standard);
            }

            var newAdditionalCommands = new List<ICqlCommand>();

            if (mode == SaveChangesMode.OneByOne)
            {
                foreach (var table in _tables)
                    if ((((tableType & TableType.Counter) == TableType.Counter) &&
                         tableTypes[table.Key] == TableType.Counter)
                        ||
                        (((tableType & TableType.Standard) == TableType.Standard) &&
                         tableTypes[table.Key] == TableType.Standard))
                        table.Value.GetMutationTracker().SaveChangesOneByOne(this, table.Key);

                foreach (var cplDels in _additionalCommands)
                    if ((tableType & TableType.Counter) == TableType.Counter)
                    {
                        if (tableTypes[cplDels.GetTable().GetTableName()] == TableType.Counter)
                            cplDels.Execute();
                        else if (tableType == TableType.Counter)
                            newAdditionalCommands.Add(cplDels);
                    }
                    else if ((tableType & TableType.Standard) == TableType.Standard)
                    {
                        if (tableTypes[cplDels.GetTable().GetTableName()] == TableType.Standard)
                            cplDels.Execute();
                        else if (tableType == TableType.Standard)
                            newAdditionalCommands.Add(cplDels);
                    }
            }
            else
            {

                if (((tableType & TableType.Counter) == TableType.Counter))
                {
                    var counterBatchScript = new StringBuilder();
                    foreach (var table in _tables)
                        if (tableTypes[table.Key] == TableType.Counter)
                            table.Value.GetMutationTracker().AppendChangesToBatch(counterBatchScript, table.Key);

                    foreach (var cplDels in _additionalCommands)
                        if (tableTypes[cplDels.GetTable().GetTableName()] == TableType.Counter)
                            counterBatchScript.AppendLine(cplDels.GetCql() + ";");
                        else if (tableType == TableType.Counter)
                            newAdditionalCommands.Add(cplDels);

                    if (counterBatchScript.Length != 0)
                    {
                        ExecuteWriteQuery("BEGIN COUNTER BATCH\r\n" + counterBatchScript.ToString() + "\r\nAPPLY BATCH");
                        foreach (var table in _tables)
                            if (tableTypes[table.Key] == TableType.Counter)
                                table.Value.GetMutationTracker().BatchCompleted();
                    }
                }


                if (((tableType & TableType.Standard) == TableType.Standard))
                {
                    var batchScript = new StringBuilder();
                    foreach (var table in _tables)
                        if (tableTypes[table.Key] == TableType.Standard)
                            table.Value.GetMutationTracker().AppendChangesToBatch(batchScript, table.Key);

                    foreach (var cplDels in _additionalCommands)
                        if (tableTypes[cplDels.GetTable().GetTableName()] == TableType.Standard)
                            batchScript.AppendLine(cplDels.GetCql() + ";");
                        else if (tableType == TableType.Standard)
                            newAdditionalCommands.Add(cplDels);

                    if (batchScript.Length != 0)
                    {
                        ExecuteWriteQuery("BEGIN BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY BATCH");
                        foreach (var table in _tables)
                            if (tableTypes[table.Key] == TableType.Standard)
                                table.Value.GetMutationTracker().BatchCompleted();
                    }
                }
            }
            _additionalCommands = newAdditionalCommands;
        }

        private List<ICqlCommand> _additionalCommands = new List<ICqlCommand>();

        public void AppendCommand(ICqlCommand cqlCommand)
        {
            _additionalCommands.Add(cqlCommand);
        }
    }
}
