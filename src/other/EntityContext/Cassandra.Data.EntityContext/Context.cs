using Cassandra.Data.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Cassandra.Data.EntityContext
{
    public class Context
    {
        private readonly Dictionary<string, IMutationTracker> _mutationTrackers = new Dictionary<string, IMutationTracker>();
        private readonly Dictionary<string, ITable> _tables = new Dictionary<string, ITable>();
        internal List<CqlCommand> _additionalCommands = new List<CqlCommand>();
        private string _keyspaceName;
        private ISession _managedSession;

        /// <summary>
        /// Gets name of keyspace.
        /// </summary>
        public string Keyspace
        {
            get { return _keyspaceName; }
        }

        public Context(ISession cqlSession)
        {
            Initialize(cqlSession);
        }

        private void Initialize(ISession cqlConnection)
        {
            if (cqlConnection == null)
                return;
            _managedSession = cqlConnection;
            _keyspaceName = cqlConnection.Keyspace;
        }

        public void CreateTablesIfNotExist()
        {
            foreach (KeyValuePair<string, ITable> table in _tables)
            {
                try
                {
                    table.Value.Create();
                }
                catch (AlreadyExistsException)
                {
                    //already exists
                }
            }
        }

        internal static string QuotedGlobalTableName(string calcedTableName, string keyspaceName)
        {
            if (keyspaceName != null)
                return keyspaceName.QuoteIdentifier() + "." + calcedTableName.QuoteIdentifier();
            return calcedTableName.QuoteIdentifier();
        }

        public ContextTable<TEntity> AddTable<TEntity>(string tableName = null, string keyspaceName = null) where TEntity : class
        {
            string tn = QuotedGlobalTableName(Table<TEntity>.CalculateName(tableName), keyspaceName);
            if (_tables.ContainsKey(tn))
                return new ContextTable<TEntity>((Table<TEntity>) _tables[tn], this);
            var table = new Table<TEntity>(_managedSession, tableName, keyspaceName);
            _tables.Add(tn, table);
            _mutationTrackers.Add(tn, new MutationTracker<TEntity>());
            return new ContextTable<TEntity>(table, this);
        }

        public bool HasTable<TEntity>(string tableName = null, string keyspaceName = null) where TEntity : class
        {
            string tn = QuotedGlobalTableName(Table<TEntity>.CalculateName(tableName), keyspaceName);
            return _tables.ContainsKey(tn);
        }

        public ContextTable<TEntity> GetTable<TEntity>(string tableName = null, string keyspaceName = null) where TEntity : class
        {
            string tn = QuotedGlobalTableName(Table<TEntity>.CalculateName(tableName), keyspaceName);
            return new ContextTable<TEntity>((Table<TEntity>) _tables[tn], this);
        }

        internal RowSet ExecuteReadQuery(string cqlQuery, ConsistencyLevel consistencyLevel, bool enableTraceing)
        {
            return _managedSession.Execute(new SimpleStatement(cqlQuery).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel));
        }

        internal IAsyncResult BeginExecuteReadQuery(string cqlQuery, ConsistencyLevel consistencyLevel, bool enableTraceing, object tag,
                                                    AsyncCallback callback, object state)
        {
            return _managedSession.BeginExecute(new SimpleStatement(cqlQuery).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel), tag,
                                                callback, state);
        }

        internal RowSet EndExecuteReadQuery(IAsyncResult ar)
        {
            return _managedSession.EndExecute(ar);
        }

        internal RowSet ExecuteWriteQuery(string cqlQuery, object[] values, ConsistencyLevel consistencyLevel, bool enableTraceing)
        {
            var statement = _managedSession.Prepare(cqlQuery);
            return _managedSession.Execute(statement.Bind(values).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel));
        }

        internal IAsyncResult BeginExecuteWriteQuery(string cqlQuery, object[] values, ConsistencyLevel consistencyLevel, bool enableTraceing,
                                                     object tag, AsyncCallback callback, object state)
        {
            return
                _managedSession.BeginExecute(
                    new SimpleStatement(cqlQuery).BindObjects(values).EnableTracing(enableTraceing).SetConsistencyLevel(consistencyLevel), tag,
                    callback, state);
        }

        internal RowSet EndExecuteWriteQuery(IAsyncResult ar)
        {
            return _managedSession.EndExecute(ar);
        }

        public IAsyncResult BeginSaveChangesBatch(TableType tableType, ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            string cql;
            if (_managedSession.BinaryProtocolVersion > 1)
                return BeginSaveChangesBatchV2(tableType, consistencyLevel, callback, state);
            return BeginSaveChangesBatchV1(tableType, consistencyLevel, callback, state, out cql);
        }

        public override string ToString()
        {
            string cql;
            BeginSaveChangesBatchV1(TableType.Standard, ConsistencyLevel.Any, null, null, out cql);
            return cql;
        }

        private IAsyncResult BeginSaveChangesBatchV1(TableType tableType, ConsistencyLevel consistencyLevel, AsyncCallback callback, object state,
                                                     out string cql)
        {
            cql = null;
            if (tableType == TableType.All)
                throw new ArgumentOutOfRangeException("tableType");

            var tableTypes = new Dictionary<string, TableType>();

            foreach (KeyValuePair<string, ITable> table in _tables)
            {
                bool isCounter = false;
                List<MemberInfo> props = table.Value.GetEntityType().GetPropertiesOrFields();
                foreach (MemberInfo prop in props)
                {
                    Type tpy = prop.GetTypeFromPropertyOrField();
                    if (
                        prop.GetCustomAttributes(typeof (CounterAttribute), true).FirstOrDefault() as
                        CounterAttribute != null)
                    {
                        isCounter = true;
                        break;
                    }
                }
                tableTypes.Add(table.Key, isCounter ? TableType.Counter : TableType.Standard);
            }

            var newAdditionalCommands = new List<CqlCommand>();
            if (((tableType & TableType.Counter) == TableType.Counter))
            {
                bool enableTracing = false;
                var counterBatchScript = new StringBuilder();
                foreach (KeyValuePair<string, ITable> table in _tables)
                    if (tableTypes[table.Key] == TableType.Counter)
                        enableTracing |= _mutationTrackers[table.Key].AppendChangesToBatch(counterBatchScript, table.Key);

                foreach (CqlCommand additional in _additionalCommands)
                    if (tableTypes[additional.GetTable().GetQuotedTableName()] == TableType.Counter)
                    {
                        counterBatchScript.AppendLine(additional + ";");
                        enableTracing |= additional.IsTracing;
                    }
                    else if (tableType == TableType.Counter)
                        newAdditionalCommands.Add(additional);

                if (counterBatchScript.Length != 0)
                {
                    cql = "BEGIN COUNTER BATCH\r\n" + counterBatchScript + "\r\nAPPLY BATCH";
                    return BeginExecuteWriteQuery(
                        cql, null, consistencyLevel, enableTracing,
                        new CqlSaveTag {TableTypes = tableTypes, TableType = TableType.Counter, NewAdditionalCommands = newAdditionalCommands},
                        callback, state);
                }
            }


            if (((tableType & TableType.Standard) == TableType.Standard))
            {
                bool enableTracing = false;
                var batchScript = new StringBuilder();
                foreach (KeyValuePair<string, ITable> table in _tables)
                    if (tableTypes[table.Key] == TableType.Standard)
                        enableTracing |= _mutationTrackers[table.Key].AppendChangesToBatch(batchScript, table.Key);

                foreach (CqlCommand additional in _additionalCommands)
                    if (tableTypes[additional.GetTable().GetQuotedTableName()] == TableType.Standard)
                    {
                        enableTracing |= additional.IsTracing;
                        batchScript.AppendLine(additional + ";");
                    }
                    else if (tableType == TableType.Standard)
                        newAdditionalCommands.Add(additional);

                if (batchScript.Length != 0)
                {
                    cql ="BEGIN BATCH\r\n" + batchScript.ToString() + "APPLY BATCH";
                    return BeginExecuteWriteQuery(cql, null, consistencyLevel, enableTracing,
                        new CqlSaveTag() { TableTypes = tableTypes, TableType = TableType.Standard, NewAdditionalCommands = newAdditionalCommands }, callback, state);
                }
            }

            return null;
        }

        private IAsyncResult BeginSaveChangesBatchV2(TableType tableType, ConsistencyLevel consistencyLevel, AsyncCallback callback, object state)
        {
            if (tableType == TableType.All)
                throw new ArgumentOutOfRangeException("tableType");

            var tableTypes = new Dictionary<string, TableType>();

            foreach (KeyValuePair<string, ITable> table in _tables)
            {
                bool isCounter = false;
                List<MemberInfo> props = table.Value.GetEntityType().GetPropertiesOrFields();
                foreach (MemberInfo prop in props)
                {
                    Type tpy = prop.GetTypeFromPropertyOrField();
                    if (
                        prop.GetCustomAttributes(typeof (CounterAttribute), true).FirstOrDefault() as
                        CounterAttribute != null)
                    {
                        isCounter = true;
                        break;
                    }
                }
                tableTypes.Add(table.Key, isCounter ? TableType.Counter : TableType.Standard);
            }

            var newAdditionalCommands = new List<CqlCommand>();
            if (((tableType & TableType.Counter) == TableType.Counter))
            {
                bool enableTracing = false;
                var counterBatchScript = new BatchStatement();
                foreach (KeyValuePair<string, ITable> table in _tables)
                    if (tableTypes[table.Key] == TableType.Counter)
                        enableTracing |= _mutationTrackers[table.Key].AppendChangesToBatch(counterBatchScript, table.Key);

                foreach (CqlCommand additional in _additionalCommands)
                    if (tableTypes[additional.GetTable().GetQuotedTableName()] == TableType.Counter)
                    {
                        counterBatchScript.Add(additional);
                        enableTracing |= additional.IsTracing;
                    }
                    else if (tableType == TableType.Counter)
                        newAdditionalCommands.Add(additional);

                if (!counterBatchScript.IsEmpty)
                {
                    return
                        _managedSession.BeginExecute(
                            counterBatchScript.SetBatchType(BatchType.Counter).SetConsistencyLevel(consistencyLevel).EnableTracing(enableTracing),
                            new CqlSaveTag {TableTypes = tableTypes, TableType = TableType.Counter, NewAdditionalCommands = newAdditionalCommands},
                            callback, state);
                }
            }


            if (((tableType & TableType.Standard) == TableType.Standard))
            {
                bool enableTracing = false;
                var batchScript = new BatchStatement();
                foreach (KeyValuePair<string, ITable> table in _tables)
                    if (tableTypes[table.Key] == TableType.Standard)
                        enableTracing |= _mutationTrackers[table.Key].AppendChangesToBatch(batchScript, table.Key);

                foreach (CqlCommand additional in _additionalCommands)
                    if (tableTypes[additional.GetTable().GetQuotedTableName()] == TableType.Standard)
                    {
                        enableTracing |= additional.IsTracing;
                        batchScript.Add(additional);
                    }
                    else if (tableType == TableType.Standard)
                        newAdditionalCommands.Add(additional);

                if (!batchScript.IsEmpty)
                {
                    return
                        _managedSession.BeginExecute(
                            batchScript.SetBatchType(BatchType.Logged).SetConsistencyLevel(consistencyLevel).EnableTracing(enableTracing),
                            new CqlSaveTag {TableTypes = tableTypes, TableType = TableType.Standard, NewAdditionalCommands = newAdditionalCommands},
                            callback, state);
                }
            }

            return null;
        }

        public void EndSaveChangesBatch(IAsyncResult ar)
        {
            RowSet res = EndExecuteWriteQuery(ar);

            var tag = (CqlSaveTag) Session.GetTag(ar);
            foreach (KeyValuePair<string, ITable> table in _tables)
                if (tag.TableTypes[table.Key] == tag.TableType)
                    _mutationTrackers[table.Key].BatchCompleted(res.Info.QueryTrace);
            _additionalCommands = tag.NewAdditionalCommands;
        }

        public void SaveChanges(ConsistencyLevel consistencyLevel, SaveChangesMode mode = SaveChangesMode.Batch, TableType tableType = TableType.All)
        {
            saveChanges(consistencyLevel, mode, tableType);
        }

        /// <summary>
        /// With default consistency level.
        /// </summary>
        /// <param name="mode"></param>
        /// <param name="tableType"></param>
        public void SaveChanges(SaveChangesMode mode = SaveChangesMode.Batch, TableType tableType = TableType.All)
        {
            saveChanges(_managedSession.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), mode, tableType);
        }

        private void saveChanges(ConsistencyLevel consistencyLevel, SaveChangesMode mode, TableType tableType)
        {
            var newAdditionalCommands = new List<CqlCommand>();

            if (mode == SaveChangesMode.OneByOne)
            {
                foreach (KeyValuePair<string, ITable> table in _tables)
                    if ((((tableType & TableType.Counter) == TableType.Counter) &&
                         table.Value.GetTableType() == TableType.Counter)
                        ||
                        (((tableType & TableType.Standard) == TableType.Standard) &&
                         table.Value.GetTableType() == TableType.Standard))
                        _mutationTrackers[table.Key].SaveChangesOneByOne(this, table.Key, consistencyLevel);

                foreach (CqlCommand additional in _additionalCommands)
                {
                    if ((tableType & TableType.Counter) == TableType.Counter)
                    {
                        if (additional.GetTable().GetTableType() == TableType.Counter)
                            additional.Execute();
                        else if (tableType == TableType.Counter)
                            newAdditionalCommands.Add(additional);
                    }
                    else if ((tableType & TableType.Standard) == TableType.Standard)
                    {
                        if (additional.GetTable().GetTableType() == TableType.Standard)
                            additional.Execute();
                        else if (tableType == TableType.Standard)
                            newAdditionalCommands.Add(additional);
                    }
                }
            }
            else
            {
                if (tableType.HasFlag(TableType.Counter))
                {
                    IAsyncResult ar = BeginSaveChangesBatch(TableType.Counter, consistencyLevel, null, null);
                    if (ar != null)
                        EndSaveChangesBatch(ar);
                }
                if (tableType.HasFlag(TableType.Standard))
                {
                    IAsyncResult ar = BeginSaveChangesBatch(TableType.Standard, consistencyLevel, null, null);
                    if (ar != null)
                        EndSaveChangesBatch(ar);
                }
            }
            _additionalCommands = newAdditionalCommands;
        }

        public void AppendCommand(CqlCommand cqlCommand)
        {
            _additionalCommands.Add(cqlCommand);
        }

        public void EnableQueryTracing<TEntity>(Table<TEntity> table, TEntity entity, bool enable = true)
        {
            (_mutationTrackers[table.GetQuotedTableName()] as MutationTracker<TEntity>).EnableQueryTracing(entity, enable);
        }

        public List<QueryTrace> RetriveAllQueryTraces(ITable table)
        {
            return _mutationTrackers[table.GetQuotedTableName()].RetriveAllQueryTraces();
        }

        public QueryTrace RetriveQueryTrace<TEntity>(Table<TEntity> table, TEntity entity)
        {
            return (_mutationTrackers[table.GetQuotedTableName()] as MutationTracker<TEntity>).RetriveQueryTrace(entity);
        }

        public void Attach<TEntity>(Table<TEntity> table, TEntity entity, EntityUpdateMode updmod = EntityUpdateMode.AllOrNone,
                                    EntityTrackingMode trmod = EntityTrackingMode.KeepAttachedAfterSave)
        {
            (_mutationTrackers[table.GetQuotedTableName()] as MutationTracker<TEntity>).Attach(entity, updmod, trmod);
        }

        public void Detach<TEntity>(Table<TEntity> table, TEntity entity)
        {
            (_mutationTrackers[table.GetQuotedTableName()] as MutationTracker<TEntity>).Detach(entity);
        }

        public void Delete<TEntity>(Table<TEntity> table, TEntity entity)
        {
            (_mutationTrackers[table.GetQuotedTableName()] as MutationTracker<TEntity>).Delete(entity);
        }

        public void AddNew<TEntity>(Table<TEntity> table, TEntity entity, EntityTrackingMode trmod = EntityTrackingMode.DetachAfterSave)
        {
            (_mutationTrackers[table.GetQuotedTableName()] as MutationTracker<TEntity>).AddNew(entity, trmod);
        }

        private struct CqlSaveTag
        {
            public List<CqlCommand> NewAdditionalCommands;
            public TableType TableType;
            public Dictionary<string, TableType> TableTypes;
        }
    }
}