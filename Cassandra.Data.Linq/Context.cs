using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cassandra.Data.Linq
{
    public class ContextTable<TEntity> : Table<TEntity>
    {
        private Context _context;

        internal ContextTable(Table<TEntity> table, Context context) : base(table)
        {
            _context = context;
        }

        public void Attach(TEntity entity, EntityUpdateMode updmod = EntityUpdateMode.AllOrNone, EntityTrackingMode trmod = EntityTrackingMode.KeepAttachedAfterSave)
        {
            _context.Attach(this, entity, updmod, trmod);
        }

        public void Detach(TEntity entity)
        {
            _context.Detach(this, entity);
        }

        public void Delete(TEntity entity)
        {
            _context.Delete(this, entity);
        }

        public void AddNew(TEntity entity, EntityTrackingMode trmod = EntityTrackingMode.DetachAfterSave)
        {
            _context.AddNew(this, entity, trmod);
        }

        public void EnableQueryTracing(TEntity entity, bool enable = true)
        {
            _context.EnableQueryTracing(this, entity, enable);
        }

        public List<QueryTrace> RetriveAllQueryTraces()
        {
            return _context.RetriveAllQueryTraces(this);
        }

        public QueryTrace RetriveQueryTrace( TEntity entity)
        {
            return _context.RetriveQueryTrace(this, entity);
        }
    }

    public class Context
    {
        private Session _managedSession = null;

        private string _keyspaceName;

        public string Keyspace
        {
            get { return _keyspaceName; }
        }

        private void Initialize(Session cqlConnection)
        {
            this._managedSession = cqlConnection;
            this._keyspaceName = cqlConnection.Keyspace;
        }

        public Context(Session cqlSession)
        {
            Initialize(cqlSession);
        }

        private readonly Dictionary<string, ITable> _tables = new Dictionary<string, ITable>();
        private readonly Dictionary<string, IMutationTracker> _mutationTrackers = new Dictionary<string, IMutationTracker>();

        public void CreateTablesIfNotExist(ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {
            foreach (var table in _tables)
            {
                try
                {
                    table.Value.Create(consistencyLevel);
                }
                catch (AlreadyExistsException)
                {
                    //already exists
                }
            }
        }

        public ContextTable<TEntity> AddTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof (TEntity).Name;
            if (_tables.ContainsKey(tn))
                return new ContextTable<TEntity>((Table<TEntity>)_tables[tn], this);
            else
            {
                var table = new Table<TEntity>(_managedSession, tn);
                _tables.Add(tn, table);
                _mutationTrackers.Add(tn, new MutationTracker<TEntity>());
                return new ContextTable<TEntity>(table, this);
            }
        }

        public bool HasTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof (TEntity).Name;
            return _tables.ContainsKey(tn);
        }

        public ContextTable<TEntity> GetTable<TEntity>(string tableName = null) where TEntity : class
        {
            var tn = tableName ?? typeof (TEntity).Name;
            return new ContextTable<TEntity>((Table<TEntity>)_tables[tn], this);
        }

        internal CqlRowSet ExecuteReadQuery(string cqlQuery, ConsistencyLevel consistencyLevel,  bool enableTraceing)
        {
            return _managedSession.Execute(new SimpleStatement(cqlQuery).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel));
        }

        internal IAsyncResult BeginExecuteReadQuery(string cqlQuery, ConsistencyLevel consistencyLevel, bool enableTraceing, object tag, AsyncCallback callback, object state)
        {
            return _managedSession.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel), tag, callback, state);
        }

        internal CqlRowSet EndExecuteReadQuery(IAsyncResult ar)
        {
            return _managedSession.EndExecute(ar);
        }

        internal CqlRowSet ExecuteWriteQuery(string cqlQuery, ConsistencyLevel consistencyLevel, bool enableTraceing)
        {
            return _managedSession.Execute(new SimpleStatement(cqlQuery).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel));
        }

        internal IAsyncResult BeginExecuteWriteQuery(string cqlQuery, ConsistencyLevel consistencyLevel, bool enableTraceing, object tag, AsyncCallback callback, object state)
        {
            return _managedSession.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel), tag, callback, state);
        }

        internal CqlRowSet EndExecuteWriteQuery(IAsyncResult ar)
        {
            return _managedSession.EndExecute(ar);
        }

        private struct CqlSaveTag
        {
            public Dictionary<string, TableType> TableTypes;
            public TableType TableType;
            public List<ICqlCommand> NewAdditionalCommands;
        }

        public IAsyncResult BeginSaveChangesBatch(TableType tableType, ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
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
                tableTypes.Add(table.Key, isCounter ? TableType.Counter : TableType.Standard);
            }

            var newAdditionalCommands = new List<ICqlCommand>();
            if (((tableType & TableType.Counter) == TableType.Counter))
            {
                bool enableTracing = false;
                var counterBatchScript = new StringBuilder();
                foreach (var table in _tables)
                    if (tableTypes[table.Key] == TableType.Counter)
                        enableTracing|=_mutationTrackers[table.Key].AppendChangesToBatch(counterBatchScript, table.Key);

                foreach (var additional in _additionalCommands)
                    if (tableTypes[additional.GetTable().GetTableName()] == TableType.Counter)
                    {
                        counterBatchScript.AppendLine(additional.GetCql() + ";");
                        enableTracing |= additional.IsQueryTraceEnabled();
                    }
                    else if (tableType == TableType.Counter)
                        newAdditionalCommands.Add(additional);

                if (counterBatchScript.Length != 0)
                {
                    return BeginExecuteWriteQuery(
                        "BEGIN COUNTER BATCH\r\n" + counterBatchScript.ToString() + "\r\nAPPLY BATCH", consistencyLevel, enableTracing,
                            new CqlSaveTag() { TableTypes = tableTypes, TableType = TableType.Counter, NewAdditionalCommands = newAdditionalCommands }, callback, state);
                }
            }


            if (((tableType & TableType.Standard) == TableType.Standard))
            {
                bool enableTracing = false;
                var batchScript = new StringBuilder();
                foreach (var table in _tables)
                    if (tableTypes[table.Key] == TableType.Standard)
                        enableTracing |= _mutationTrackers[table.Key].AppendChangesToBatch(batchScript, table.Key);

                foreach (var additional in _additionalCommands)
                    if (tableTypes[additional.GetTable().GetTableName()] == TableType.Standard)
                    {
                        enableTracing |= additional.IsQueryTraceEnabled();
                        batchScript.AppendLine(additional.GetCql() + ";");
                    }
                    else if (tableType == TableType.Standard)
                        newAdditionalCommands.Add(additional);

                if (batchScript.Length != 0)
                {
                    return BeginExecuteWriteQuery("BEGIN BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY BATCH",consistencyLevel, enableTracing,
                        new CqlSaveTag() { TableTypes = tableTypes, TableType = TableType.Standard, NewAdditionalCommands = newAdditionalCommands }, callback, state);
                }
            }

            throw new InvalidOperationException();
        }

        public void EndSaveChangesBatch(IAsyncResult ar)
        {
            var res = EndExecuteWriteQuery(ar);

            var tag = (CqlSaveTag) Session.GetTag(ar);
            foreach (var table in _tables)
                if (tag.TableTypes[table.Key] == tag.TableType)
                    _mutationTrackers[table.Key].BatchCompleted(res.QueryTrace);
            _additionalCommands = tag.NewAdditionalCommands;
        }

        public void SaveChanges(SaveChangesMode mode = SaveChangesMode.Batch, TableType tableType = TableType.All,ConsistencyLevel consistencyLevel = ConsistencyLevel.Default)
        {

            var newAdditionalCommands = new List<ICqlCommand>();

            if (mode == SaveChangesMode.OneByOne)
            {
                foreach (var table in _tables)
                    if ((((tableType & TableType.Counter) == TableType.Counter) &&
                         table.Value.GetTableType() == TableType.Counter)
                        ||
                        (((tableType & TableType.Standard) == TableType.Standard) &&
                         table.Value.GetTableType() == TableType.Standard))
                        _mutationTrackers[table.Key].SaveChangesOneByOne(this, table.Key, consistencyLevel);

                foreach (var additional in _additionalCommands)
                {
                    if ((tableType & TableType.Counter) == TableType.Counter)
                    {
                        if (additional.GetTable().GetTableType() == TableType.Counter)
                            additional.Execute(consistencyLevel);
                        else if (tableType == TableType.Counter)
                            newAdditionalCommands.Add(additional);
                    }
                    else if ((tableType & TableType.Standard) == TableType.Standard)
                    {
                        if (additional.GetTable().GetTableType() == TableType.Standard)
                            additional.Execute(consistencyLevel);
                        else if (tableType == TableType.Standard)
                            newAdditionalCommands.Add(additional);
                    }
                }
            }
            else
            {
                if (((tableType & TableType.Counter) == TableType.Counter))
                {
                    bool enableTracing = false;
                    var counterBatchScript = new StringBuilder();
                    foreach (var table in _tables)
                        if (table.Value.GetTableType() == TableType.Counter)
                            enableTracing |= _mutationTrackers[table.Key].AppendChangesToBatch(counterBatchScript, table.Key);

                    foreach (var additional in _additionalCommands)
                        if (additional.GetTable().GetTableType()== TableType.Counter)
                        {
                            enableTracing |= additional.IsQueryTraceEnabled();
                            counterBatchScript.AppendLine(additional.GetCql() + ";");
                        }
                        else if (tableType == TableType.Counter)
                            newAdditionalCommands.Add(additional);

                    if (counterBatchScript.Length != 0)
                    {
                        var res = ExecuteWriteQuery("BEGIN COUNTER BATCH\r\n" + counterBatchScript.ToString() + "\r\nAPPLY BATCH",consistencyLevel, enableTracing);
                        foreach (var table in _tables)
                            if (table.Value.GetTableType() == TableType.Counter)
                                _mutationTrackers[table.Key].BatchCompleted(res.QueryTrace);

                        foreach (var additional in _additionalCommands)
                            additional.SetQueryTrace(res.QueryTrace);
                    }
                }


                if (((tableType & TableType.Standard) == TableType.Standard))
                {
                    bool enableTracing = false;
                    var batchScript = new StringBuilder();
                    foreach (var table in _tables)
                        if (table.Value.GetTableType() == TableType.Standard)
                            enableTracing |= _mutationTrackers[table.Key].AppendChangesToBatch(batchScript, table.Key);

                    foreach (var additional in _additionalCommands)
                        if (additional.GetTable().GetTableType() == TableType.Standard)
                        {
                            enableTracing |= additional.IsQueryTraceEnabled();
                            batchScript.AppendLine(additional.GetCql() + ";");
                        }
                        else if (tableType == TableType.Standard)
                            newAdditionalCommands.Add(additional);

                    if (batchScript.Length != 0)
                    {
                        var res = ExecuteWriteQuery("BEGIN BATCH\r\n" + batchScript.ToString() + "\r\nAPPLY BATCH", consistencyLevel, enableTracing);
                        foreach (var table in _tables)
                            if (table.Value.GetTableType() == TableType.Standard)
                                _mutationTrackers[table.Key].BatchCompleted(res.QueryTrace);

                        foreach (var additional in _additionalCommands)
                            additional.SetQueryTrace(res.QueryTrace);
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

        public void EnableQueryTracing<TEntity>(Table<TEntity> table, TEntity entity, bool enable = true)
        {
            (_mutationTrackers[table.GetTableName()] as MutationTracker<TEntity>).EnableQueryTracing(entity, enable);
        }

        public List<QueryTrace> RetriveAllQueryTraces(ITable table)
        {
            return _mutationTrackers[table.GetTableName()].RetriveAllQueryTraces();
        }

        public QueryTrace RetriveQueryTrace<TEntity>(Table<TEntity> table, TEntity entity)
        {
            return (_mutationTrackers[table.GetTableName()] as MutationTracker<TEntity>).RetriveQueryTrace(entity);
        }

        public void Attach<TEntity>(Table<TEntity> table, TEntity entity, EntityUpdateMode updmod = EntityUpdateMode.AllOrNone, EntityTrackingMode trmod = EntityTrackingMode.KeepAttachedAfterSave)
        {
            (_mutationTrackers[table.GetTableName()] as MutationTracker<TEntity>).Attach(entity, updmod, trmod);
        }

        public void Detach<TEntity>(Table<TEntity> table, TEntity entity)
        {
            (_mutationTrackers[table.GetTableName()] as MutationTracker<TEntity>).Detach(entity);
        }

        public void Delete<TEntity>(Table<TEntity> table, TEntity entity)
        {
            (_mutationTrackers[table.GetTableName()] as MutationTracker<TEntity>).Delete(entity);
        }

        public void AddNew<TEntity>(Table<TEntity> table, TEntity entity, EntityTrackingMode trmod = EntityTrackingMode.DetachAfterSave)
        {
            (_mutationTrackers[table.GetTableName()] as MutationTracker<TEntity>).AddNew(entity, trmod);
        }
    }
}
