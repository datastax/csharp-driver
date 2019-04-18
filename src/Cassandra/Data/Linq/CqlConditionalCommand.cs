using System;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Tasks;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents an INSERT/UPDATE/DELETE command with support for Lightweight transactions.
    /// </summary>
    public class CqlConditionalCommand<TEntity>: CqlCommand
    {
        private readonly MapperFactory _mapperFactory;
        private readonly CqlCommand _origin;

        internal CqlConditionalCommand(CqlCommand origin, MapperFactory mapperFactory)
            : base(origin.Expression, origin.Table, origin.StatementFactory, origin.PocoData)
        {
            _mapperFactory = mapperFactory;
            _origin = origin;
            //Copy the Statement properties from origin
            _origin.CopyQueryPropertiesTo(this);
        }

        protected internal override string GetCql(out object[] values)
        {
            return _origin.GetCql(out values);
        }

        /// <summary>
        /// Asynchronously executes a conditional query and returns information whether it was applied.
        /// </summary>
        public new Task<AppliedInfo<TEntity>> ExecuteAsync()
        {
            return ExecuteWithProfileAsync(null);
        }

        /// <summary>
        /// Executes a conditional query and returns information whether it was applied.
        /// </summary>
        /// <returns>An instance of AppliedInfo{TEntity}</returns>
        public new AppliedInfo<TEntity> Execute()
        {
            return ExecuteWithProfile(null);
        }

        /// <summary>
        /// Asynchronously executes a conditional query with the provided execution profile and returns information whether it was applied.
        /// </summary>
        public new Task<AppliedInfo<TEntity>> ExecuteAsync(string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            return ExecuteWithProfileAsync(executionProfile);
        }

        /// <summary>
        /// Executes a conditional query with the provided execution profile and returns information whether it was applied.
        /// </summary>
        /// <returns>An instance of AppliedInfo{TEntity}</returns>
        public new AppliedInfo<TEntity> Execute(string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            return ExecuteWithProfile(executionProfile);
        }

        private async Task<AppliedInfo<TEntity>> ExecuteWithProfileAsync(string executionProfile)
        {
            object[] values;
            var cql = GetCql(out values);
            var session = GetTable().GetSession();
            var stmt = await StatementFactory.GetStatementAsync(session, Cql.New(cql, values)).ConfigureAwait(false);
            this.CopyQueryPropertiesTo(stmt);
            var rs = executionProfile != null
                ? await session.ExecuteAsync(stmt, executionProfile).ConfigureAwait(false) 
                : await session.ExecuteAsync(stmt).ConfigureAwait(false);
            return AppliedInfo<TEntity>.FromRowSet(_mapperFactory, cql, rs);
        }

        private AppliedInfo<TEntity> ExecuteWithProfile(string executionProfile)
        {
            var queryAbortTimeout = GetTable().GetSession().Cluster.Configuration.ClientOptions.QueryAbortTimeout;
            var task = ExecuteAsync(executionProfile);
            return TaskHelper.WaitToComplete(task, queryAbortTimeout);
        }

        public new CqlConditionalCommand<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlConditionalCommand<TEntity> SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetSerialConsistencyLevel(consistencyLevel);
            return this;
        }

        /// <summary>
        /// Sets the time for data in a column to expire (TTL) for INSERT and UPDATE commands.
        /// </summary>
        /// <param name="seconds">Amount of seconds.</param>
        /// <returns>This instance.</returns>
        public new CqlConditionalCommand<TEntity> SetTTL(int seconds)
        {
            _origin.SetTTL(seconds);
            return this;
        }

        /// <summary>
        /// Sets the timestamp associated with this statement execution.
        /// </summary>
        /// <returns>This instance.</returns>
        public new CqlConditionalCommand<TEntity> SetTimestamp(DateTimeOffset timestamp)
        {
            _origin.SetTimestamp(timestamp);
            return this;
        }

        /// <summary>
        /// Generates and returns the Cql query
        /// </summary>
        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }
    }
}
