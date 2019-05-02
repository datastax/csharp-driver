using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Mapping.Statements;
using Cassandra.Tasks;

namespace Cassandra.Mapping
{
    /// <summary>
    /// The default CQL client implementation which uses the DataStax driver <see cref="ISession"/> provided in the constructor
    /// for running queries against a Cassandra cluster.
    /// </summary>
    /// <seealso cref="IMapper"/>
    /// <inheritdoc />
    public class Mapper : IMapper
    {
        private readonly ISession _session;
        private readonly MapperFactory _mapperFactory;
        private readonly StatementFactory _statementFactory;
        private readonly CqlGenerator _cqlGenerator;
        private readonly int _queryAbortTimeout = 3000;

        /// <summary>
        /// Creates a new instance of the mapper using the configuration provided
        /// </summary>
        /// <param name="session">Session to be used to execute the statements</param>
        /// <param name="config">Mapping definitions for the POCOs</param>
        public Mapper(ISession session, MappingConfiguration config) 
            : this(session, config.MapperFactory, config.StatementFactory, new CqlGenerator(config.MapperFactory.PocoDataFactory))
        {

        }

        /// <summary>
        /// Creates a new instance of the mapper using <see cref="MappingConfiguration.Global"/> mapping definitions.
        /// </summary>
        public Mapper(ISession session) : this(session, MappingConfiguration.Global)
        {
            
        }

        internal Mapper(ISession session, MapperFactory mapperFactory, StatementFactory statementFactory, CqlGenerator cqlGenerator)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _mapperFactory = mapperFactory ?? throw new ArgumentNullException(nameof(mapperFactory));
            _statementFactory = statementFactory ?? throw new ArgumentNullException(nameof(statementFactory));
            _cqlGenerator = cqlGenerator ?? throw new ArgumentNullException(nameof(cqlGenerator));
            _queryAbortTimeout = session.Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout;
        }

        /// <summary>
        /// Executes asynchronously and uses the delegate to adapt the RowSet into the return value.
        /// </summary>
        private async Task<TResult> ExecuteAsyncAndAdapt<TResult>(Cql cql, Func<Statement, RowSet, TResult> adaptation)
        {
            var stmt = await _statementFactory.GetStatementAsync(_session, cql).ConfigureAwait(false);
            var rs = await ExecuteStatementAsync(stmt, cql.ExecutionProfile).ConfigureAwait(false);
            return adaptation(stmt, rs);
        }

        /// <inheritdoc />
        public Task<IEnumerable<T>> FetchAsync<T>(CqlQueryOptions options = null)
        {
            return FetchAsync<T>(Cql.New(string.Empty, new object[0], options ?? CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<IEnumerable<T>> FetchAsync<T>(string cql, params object[] args)
        {
            return FetchAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<IEnumerable<T>> FetchAsync<T>(Cql cql)
        {
            //Use ExecuteAsyncAndAdapt with a delegate to handle the adaptation from RowSet to IEnumerable<T>
            _cqlGenerator.AddSelect<T>(cql);
            return ExecuteAsyncAndAdapt(cql, (s, rs) =>
            {
                var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                return rs.Select(mapper);
            });
        }

        /// <inheritdoc />
        public Task<IPage<T>> FetchPageAsync<T>(Cql cql)
        {
            if (cql == null)
            {
                throw new ArgumentNullException("cql");
            }
            cql.AutoPage = false;
            _cqlGenerator.AddSelect<T>(cql);
            return ExecuteAsyncAndAdapt<IPage<T>>(cql, (stmt, rs) =>
            {
                var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                return new Page<T>(rs.Select(mapper), stmt.PagingState, rs.PagingState);
            });
        }

        /// <inheritdoc />
        public Task<IPage<T>> FetchPageAsync<T>(CqlQueryOptions options = null)
        {
            return FetchPageAsync<T>(Cql.New(string.Empty, new object[0], options ?? new CqlQueryOptions()));
        }

        /// <inheritdoc />
        public Task<IPage<T>> FetchPageAsync<T>(int pageSize, byte[] pagingState, string query, object[] args)
        {
            return FetchPageAsync<T>(Cql.New(query, args, new CqlQueryOptions().SetPageSize(pageSize).SetPagingState(pagingState)));
        }

        /// <inheritdoc />
        public Task<T> SingleAsync<T>(string cql, params object[] args)
        {
            return SingleAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<T> SingleAsync<T>(Cql cql)
        {
            _cqlGenerator.AddSelect<T>(cql);
            return ExecuteAsyncAndAdapt(cql, (s, rs) =>
            {
                var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                return mapper(rs.Single());
            });
        }

        /// <inheritdoc />
        public Task<T> SingleOrDefaultAsync<T>(string cql, params object[] args)
        {
            return SingleOrDefaultAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<T> SingleOrDefaultAsync<T>(Cql cql)
        {
            _cqlGenerator.AddSelect<T>(cql);
            return ExecuteAsyncAndAdapt(cql, (s, rs) =>
            {
                var row = rs.SingleOrDefault();
                // Map to return type
                if (row == null)
                {
                    return default(T);
                }
                var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                return mapper(row);
            });
        }

        /// <inheritdoc />
        public Task<T> FirstAsync<T>(string cql, params object[] args)
        {
            return FirstAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<T> FirstAsync<T>(Cql cql)
        {
            _cqlGenerator.AddSelect<T>(cql);
            return ExecuteAsyncAndAdapt(cql, (s, rs) =>
            {
                var row = rs.First();
                // Map to return type
                var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                return mapper(row);
            });
        }

        /// <inheritdoc />
        public Task<T> FirstOrDefaultAsync<T>(string cql, params object[] args)
        {
            return FirstOrDefaultAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<T> FirstOrDefaultAsync<T>(Cql cql)
        {
            _cqlGenerator.AddSelect<T>(cql);
            return ExecuteAsyncAndAdapt(cql, (s, rs) =>
            {
                var row = rs.FirstOrDefault();
                // Map to return type
                if (row == null)
                {
                    return default(T);
                }
                var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                return mapper(row);
            });
        }

        /// <inheritdoc />
        public Task InsertAsync<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            return InsertAsync(poco, true, queryOptions);
        }

        /// <inheritdoc />
        public Task InsertAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            return InsertAsync(poco, executionProfile, true, queryOptions);
        }

        /// <inheritdoc />
        public Task InsertAsync<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            return InsertAsync(poco, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public Task InsertAsync<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            return InsertAsync(poco, executionProfile, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public Task InsertAsync<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            return InsertAsync(poco, Configuration.DefaultExecutionProfileName, insertNulls, ttl, queryOptions);
        }

        /// <inheritdoc />
        public Task InsertAsync<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }
            
            var pocoData = _mapperFactory.PocoDataFactory.GetPocoData<T>();
            var queryIdentifier = $"INSERT ID {pocoData.KeyspaceName}/{pocoData.TableName}";
            var getBindValues = _mapperFactory.GetValueCollector<T>(queryIdentifier);
            //get values first to identify null values
            var values = getBindValues(poco);
            //generate INSERT query based on null values (if insertNulls set)
            var cql = _cqlGenerator.GenerateInsert<T>(insertNulls, values, out var queryParameters, ttl: ttl);
            var cqlInstance = Cql.New(cql, queryParameters, queryOptions ?? CqlQueryOptions.None).WithExecutionProfile(executionProfile);

            return ExecuteAsync(cqlInstance);
        }
        
        /// <inheritdoc />
        public Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExistsAsync(poco, true, queryOptions);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExistsAsync(poco, executionProfile, true, queryOptions);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExistsAsync(poco, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExistsAsync(poco, executionProfile, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExistsAsync(poco, Configuration.DefaultExecutionProfileName, insertNulls, ttl, queryOptions);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }
            
            var pocoData = _mapperFactory.PocoDataFactory.GetPocoData<T>();
            var queryIdentifier = $"INSERT ID {pocoData.KeyspaceName}/{pocoData.TableName}";
            var getBindValues = _mapperFactory.GetValueCollector<T>(queryIdentifier);
            //get values first to identify null values
            var values = getBindValues(poco);
            //generate INSERT query based on null values (if insertNulls set)
            var cql = _cqlGenerator.GenerateInsert<T>(insertNulls, values, out var queryParameters, true, ttl);
            var cqlInstance = Cql.New(cql, queryParameters, queryOptions ?? CqlQueryOptions.None).WithExecutionProfile(executionProfile);

            return ExecuteAsyncAndAdapt(
                cqlInstance,
                (stmt, rs) => AppliedInfo<T>.FromRowSet(_mapperFactory, cql, rs));
        }

        /// <inheritdoc />
        public Task UpdateAsync<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            return UpdateAsync(poco, Configuration.DefaultExecutionProfileName, queryOptions);
        }

        /// <inheritdoc />
        public Task UpdateAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateUpdate<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesLast: true);
            object[] values = getBindValues(poco);
            var cqlInstance = Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None).WithExecutionProfile(executionProfile);

            return ExecuteAsync(cqlInstance);
        }

        /// <inheritdoc />
        public Task UpdateAsync<T>(string cql, params object[] args)
        {
            return UpdateAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task UpdateAsync<T>(Cql cql)
        {
            _cqlGenerator.PrependUpdate<T>(cql);
            return ExecuteAsync(cql);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> UpdateIfAsync<T>(string cql, params object[] args)
        {
            return UpdateIfAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> UpdateIfAsync<T>(Cql cql)
        {
            _cqlGenerator.PrependUpdate<T>(cql);
            return ExecuteAsyncAndAdapt(cql, (stmt, rs) => AppliedInfo<T>.FromRowSet(_mapperFactory, cql.Statement, rs));
        }

        /// <inheritdoc />
        public Task DeleteAsync<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            return DeleteAsync(poco, Configuration.DefaultExecutionProfileName, queryOptions);
        }

        /// <inheritdoc />
        public Task DeleteAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }

            // Get the statement and bind values from POCO
            string cql = _cqlGenerator.GenerateDelete<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesOnly: true);
            object[] values = getBindValues(poco);
            var cqlInstance = Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None).WithExecutionProfile(executionProfile);

            return ExecuteAsync(cqlInstance);
        }
        
        /// <inheritdoc />
        public Task DeleteAsync<T>(string cql, params object[] args)
        {
            return DeleteAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task DeleteAsync<T>(Cql cql)
        {
            _cqlGenerator.PrependDelete<T>(cql);
            return ExecuteAsync(cql);
        }

        /// <inheritdoc />
        public Task ExecuteAsync(string cql, params object[] args)
        {
            return ExecuteAsync(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public async Task ExecuteAsync(Cql cql)
        {
            // Execute the statement
            var statement = await _statementFactory.GetStatementAsync(_session, cql).ConfigureAwait(false);
            await ExecuteStatementAsync(statement, cql.ExecutionProfile).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public ICqlBatch CreateBatch()
        {
            return new CqlBatch(_mapperFactory, _cqlGenerator);
        }

        /// <inheritdoc />
        public ICqlBatch CreateBatch(BatchType batchType)
        {
            return new CqlBatch(_mapperFactory, _cqlGenerator, batchType);
        }

        /// <inheritdoc />
        public void Execute(ICqlBatch batch)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(ExecuteAsync(batch), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Execute(ICqlBatch batch, string executionProfile)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(ExecuteAsync(batch, executionProfile), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public Task ExecuteAsync(ICqlBatch batch)
        {
            return ExecuteAsync(batch, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public async Task ExecuteAsync(ICqlBatch batch, string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }
            
            if (batch == null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            var batchStatement = await _statementFactory
                                       .GetBatchStatementAsync(_session, batch)
                                       .ConfigureAwait(false);

            await ExecuteStatementAsync(batchStatement, executionProfile).ConfigureAwait(false);
        }
        
        public TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value)
        {
            return _mapperFactory.TypeConverter.ConvertCqlArgument<TValue, TDatabase>(value);
        }

        /// <inheritdoc />
        public AppliedInfo<T> DeleteIf<T>(string cql, params object[] args)
        {
            return DeleteIf<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public AppliedInfo<T> DeleteIf<T>(Cql cql)
        {
            return TaskHelper.WaitToComplete(DeleteIfAsync<T>(cql), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> DeleteIfAsync<T>(string cql, params object[] args)
        {
            return DeleteIfAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> DeleteIfAsync<T>(Cql cql)
        {
            _cqlGenerator.PrependDelete<T>(cql);
            return ExecuteAsyncAndAdapt(cql, (stmt, rs) => AppliedInfo<T>.FromRowSet(_mapperFactory, cql.Statement, rs));
        }

        /// <inheritdoc />
        public IEnumerable<T> Fetch<T>(CqlQueryOptions queryOptions = null)
        {
            // Just let the SQL be auto-generated
            return Fetch<T>(Cql.New(string.Empty, new object[0], queryOptions ?? CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public IEnumerable<T> Fetch<T>(string cql, params object[] args)
        {
            return Fetch<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public IEnumerable<T> Fetch<T>(Cql cql)
        {
            //Use the async method
            var t = FetchAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        /// <inheritdoc />
        public IPage<T> FetchPage<T>(CqlQueryOptions queryOptions = null)
        {
            return FetchPage<T>(Cql.New(string.Empty, new object[0], queryOptions ?? new CqlQueryOptions()));
        }

        /// <inheritdoc />
        public IPage<T> FetchPage<T>(int pageSize, byte[] pagingState, string cql, params object[] args)
        {
            return FetchPage<T>(Cql.New(cql, args, new CqlQueryOptions().SetPageSize(pageSize).SetPagingState(pagingState)));
        }

        /// <inheritdoc />
        public IPage<T> FetchPage<T>(Cql cql)
        {
            //Use the async method
            var t = FetchPageAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        /// <inheritdoc />
        public T Single<T>(string cql, params object[] args)
        {
            return Single<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public T Single<T>(Cql cql)
        {
            //Use the async method
            var t = SingleAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        /// <inheritdoc />
        public T SingleOrDefault<T>(string cql, params object[] args)
        {
            return SingleOrDefault<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public T SingleOrDefault<T>(Cql cql)
        {
            //Use async method
            var t = SingleOrDefaultAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        /// <inheritdoc />
        public T First<T>(string cql, params object[] args)
        {
            return First<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public T First<T>(Cql cql)
        {
            //Use async method
            var t = FirstAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        /// <inheritdoc />
        public T FirstOrDefault<T>(string cql, params object[] args)
        {
            return FirstOrDefault<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public T FirstOrDefault<T>(Cql cql)
        {
            //Use async method
            var t = FirstOrDefaultAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        /// <inheritdoc />
        public void Insert<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            Insert(poco, true, queryOptions);
        }

        /// <inheritdoc />
        public void Insert<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            Insert(poco, executionProfile, true, queryOptions);
        }

        /// <inheritdoc />
        public void Insert<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            Insert(poco, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public void Insert<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            Insert(poco, executionProfile, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public void Insert<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            Insert(poco, Configuration.DefaultExecutionProfileName, insertNulls, ttl, queryOptions);
        }

        /// <inheritdoc />
        public void Insert<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }
            
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(InsertAsync(poco, executionProfile, insertNulls, ttl, queryOptions), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public AppliedInfo<T> InsertIfNotExists<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExists(poco, true, queryOptions);
        }

        /// <inheritdoc />
        public AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExists(poco, executionProfile, true, queryOptions);
        }

        /// <inheritdoc />
        public AppliedInfo<T> InsertIfNotExists<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExists(poco, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            return InsertIfNotExists(poco, executionProfile, insertNulls, null, queryOptions);
        }

        /// <inheritdoc />
        public AppliedInfo<T> InsertIfNotExists<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            return TaskHelper.WaitToComplete(InsertIfNotExistsAsync(poco, insertNulls, ttl, queryOptions), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            return TaskHelper.WaitToComplete(InsertIfNotExistsAsync(poco, executionProfile, insertNulls, ttl, queryOptions), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Update<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(UpdateAsync(poco, queryOptions), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Update<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(UpdateAsync(poco, executionProfile, queryOptions), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Update<T>(string cql, params object[] args)
        {
            Update<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public void Update<T>(Cql cql)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(UpdateAsync<T>(cql), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public AppliedInfo<T> UpdateIf<T>(string cql, params object[] args)
        {
            return UpdateIf<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public AppliedInfo<T> UpdateIf<T>(Cql cql)
        {
            //Wait async method to be completed or throw
            return TaskHelper.WaitToComplete(UpdateIfAsync<T>(cql), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Delete<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(DeleteAsync(poco, queryOptions), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Delete<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(DeleteAsync(poco, executionProfile, queryOptions), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Delete<T>(string cql, params object[] args)
        {
            Delete<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public void Delete<T>(Cql cql)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(DeleteAsync<T>(cql), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public void Execute(string cql, params object[] args)
        {
            Execute(Cql.New(cql, args, CqlQueryOptions.None));
        }

        /// <inheritdoc />
        public void Execute(Cql cql)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(ExecuteAsync(cql), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public Task<AppliedInfo<T>> ExecuteConditionalAsync<T>(ICqlBatch batch)
        {
            return ExecuteConditionalAsync<T>(batch, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public async Task<AppliedInfo<T>> ExecuteConditionalAsync<T>(ICqlBatch batch, string executionProfile)
        {
            if (executionProfile == null)
            {
                throw new ArgumentNullException(nameof(executionProfile));
            }
            
            if (batch == null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            var batchStatement = await _statementFactory
                                       .GetBatchStatementAsync(_session, batch)
                                       .ConfigureAwait(false);

            //Use the concatenation of cql strings as hash for the mapper
            var cqlString = string.Join(";", batch.Statements.Select(s => s.Statement));
            var rs = await ExecuteStatementAsync(batchStatement, executionProfile).ConfigureAwait(false);
            return AppliedInfo<T>.FromRowSet(_mapperFactory, cqlString, rs);
        }
        
        /// <inheritdoc />
        public AppliedInfo<T> ExecuteConditional<T>(ICqlBatch batch)
        {
            return TaskHelper.WaitToComplete(ExecuteConditionalAsync<T>(batch), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public AppliedInfo<T> ExecuteConditional<T>(ICqlBatch batch, string executionProfile)
        {
            return TaskHelper.WaitToComplete(ExecuteConditionalAsync<T>(batch, executionProfile), _queryAbortTimeout);
        }

        private Task<RowSet> ExecuteStatementAsync(IStatement statement, string executionProfile)
        {
            return executionProfile != null 
                ? _session.ExecuteAsync(statement, executionProfile) 
                : _session.ExecuteAsync(statement);
        }
    }
}