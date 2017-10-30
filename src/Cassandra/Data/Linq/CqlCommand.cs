//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Linq.Expressions;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Tasks;

namespace Cassandra.Data.Linq
{
    public abstract class CqlCommand : SimpleStatement
    {
        private readonly Expression _expression;
        private readonly StatementFactory _statementFactory;
        protected DateTimeOffset? _timestamp;
        protected int? _ttl;

        internal PocoData PocoData { get; }
        internal ITable Table { get; }

        /// <inheritdoc />
        public override string QueryString
        {
            get
            {
                if (base.QueryString == null)
                    InitializeStatement();
                return base.QueryString;
            }
        }

        /// <inheritdoc />
        public override object[] QueryValues
        {
            get
            {
                if (base.QueryString == null)
                    InitializeStatement();
                return base.QueryValues;
            }
        }

        internal StatementFactory StatementFactory => _statementFactory;

        public Expression Expression => _expression;

        public QueryTrace QueryTrace { get; private set; }

        internal CqlCommand(Expression expression, ITable table, StatementFactory stmtFactory, PocoData pocoData)
        {
            _expression = expression;
            Table = table;
            _statementFactory = stmtFactory;
            PocoData = pocoData;
        }

        protected internal abstract string GetCql(out object[] values);

        /// <summary>
        /// Executes the command using the <see cref="ISession"/>.
        /// </summary>
        public void Execute()
        {
            var config = GetTable().GetSession().GetConfiguration();
            var task = ExecuteAsync();
            TaskHelper.WaitToComplete(task, config.ClientOptions.QueryAbortTimeout);
        }

        public void SetQueryTrace(QueryTrace trace)
        {
            QueryTrace = trace;
        }

        public new CqlCommand SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlCommand SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetSerialConsistencyLevel(consistencyLevel);
            return this;
        }

        /// <summary>
        /// Sets the time for data in a column to expire (TTL) for INSERT and UPDATE commands .
        /// </summary>
        /// <param name="seconds">Amount of seconds</param>
        public CqlCommand SetTTL(int seconds)
        {
            _ttl = seconds;
            return this;
        }

        /// <summary>
        /// Sets the timestamp associated with this statement execution.
        /// </summary>
        /// <returns>This instance.</returns>
        public new CqlCommand SetTimestamp(DateTimeOffset timestamp)
        {
            _timestamp = timestamp;
            return this;
        }

        protected void InitializeStatement()
        {
            object[] values;
            string query = GetCql(out values);
            SetQueryString(query);
            SetValues(values);
        }

        public ITable GetTable()
        {
            return Table;
        }

        public async Task<RowSet> ExecuteAsync()
        {
            object[] values;
            var cqlQuery = GetCql(out values);
            var session = GetTable().GetSession();
            var stmt = await _statementFactory.GetStatementAsync(session, Cql.New(cqlQuery, values)).ConfigureAwait(false);
            this.CopyQueryPropertiesTo(stmt);
            return await session.ExecuteAsync(stmt).ConfigureAwait(false);
        }

        /// <summary>
        /// Starts executing the statement async
        /// </summary>
        public virtual IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            return ExecuteAsync().ToApm(callback, state);
        }

        /// <summary>
        /// Starts the async executing of the statement
        /// </summary>
        public virtual void EndExecute(IAsyncResult ar)
        {
            var task = (Task<RowSet>)ar;
            task.Wait();
        }
    }
}