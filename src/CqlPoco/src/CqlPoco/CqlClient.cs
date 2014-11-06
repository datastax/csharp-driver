using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using CqlPoco.Mapping;
using CqlPoco.Statements;
using CqlPoco.TypeConversion;

namespace CqlPoco
{
    /// <summary>
    /// The default CQL client implementation which uses the DataStax driver ISession provided in the constructor
    /// for running queries against a Cassandra cluster.
    /// </summary>
    internal class CqlClient : ICqlClient
    {
        private readonly ISession _session;
        private readonly MapperFactory _mapperFactory;
        private readonly StatementFactory _statementFactory;
        private readonly CqlGenerator _cqlGenerator;

        public CqlClient(ISession session, MapperFactory mapperFactory, StatementFactory statementFactory, CqlGenerator cqlGenerator)
        {
            if (session == null) throw new ArgumentNullException("session");
            if (mapperFactory == null) throw new ArgumentNullException("mapperFactory");
            if (statementFactory == null) throw new ArgumentNullException("statementFactory");
            if (cqlGenerator == null) throw new ArgumentNullException("cqlGenerator");

            _session = session;
            _mapperFactory = mapperFactory;
            _statementFactory = statementFactory;
            _cqlGenerator = cqlGenerator;
        }

        public Task<List<T>> FetchAsync<T>(CqlQueryOptions options = null)
        {
            return FetchAsync<T>(Cql.New(string.Empty, new object[0], options ?? CqlQueryOptions.None));
        }

        public Task<List<T>> FetchAsync<T>(string cql, params object[] args)
        {
            return FetchAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public async Task<List<T>> FetchAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = await _statementFactory.GetStatementAsync(cql).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return rows.Select(mapper).ToList();
        }

        public Task<T> SingleAsync<T>(string cql, params object[] args)
        {
            return SingleAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public async Task<T> SingleAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = await _statementFactory.GetStatementAsync(cql).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.Single();

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public Task<T> SingleOrDefaultAsync<T>(string cql, params object[] args)
        {
            return SingleOrDefaultAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public async Task<T> SingleOrDefaultAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = await _statementFactory.GetStatementAsync(cql).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.SingleOrDefault();

            // Map to return type or return default
            if (row == null)
                return default(T);

            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public Task<T> FirstAsync<T>(string cql, params object[] args)
        {
            return FirstAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public async Task<T> FirstAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = await _statementFactory.GetStatementAsync(cql).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.First();

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public Task<T> FirstOrDefaultAsync<T>(string cql, params object[] args)
        {
            return FirstOrDefaultAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public async Task<T> FirstOrDefaultAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = await _statementFactory.GetStatementAsync(cql).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.FirstOrDefault();

            // Map to return type or return default
            if (row == null)
                return default(T);

            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public Task InsertAsync<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateInsert<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            return ExecuteAsync(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public Task UpdateAsync<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateUpdate<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            return ExecuteAsync(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public Task UpdateAsync<T>(string cql, params object[] args)
        {
            return UpdateAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public Task UpdateAsync<T>(Cql cql)
        {
            _cqlGenerator.PrependUpdate<T>(cql);
            return ExecuteAsync(cql);
        }

        public Task DeleteAsync<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get the statement and bind values from POCO
            string cql = _cqlGenerator.GenerateDelete<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesOnly: true);
            object[] values = getBindValues(poco);

            return ExecuteAsync(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }
        
        public Task DeleteAsync<T>(string cql, params object[] args)
        {
            return DeleteAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public Task DeleteAsync<T>(Cql cql)
        {
            _cqlGenerator.PrependDelete<T>(cql);
            return ExecuteAsync(cql);
        }

        public Task ExecuteAsync(string cql, params object[] args)
        {
            return ExecuteAsync(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public async Task ExecuteAsync(Cql cql)
        {
            // Execute the statement
            Statement statement = await _statementFactory.GetStatementAsync(cql).ConfigureAwait(false);
            await _session.ExecuteAsync(statement).ConfigureAwait(false);
        }

        public ICqlBatch CreateBatch()
        {
            return new CqlBatch(_mapperFactory, _cqlGenerator);
        }

        public void Execute(ICqlBatch batch)
        {
            if (batch == null) throw new ArgumentNullException("batch");

            BatchStatement batchStatement = _statementFactory.GetBatchStatement(batch.Statements);
            _session.Execute(batchStatement);
        }

        public async Task ExecuteAsync(ICqlBatch batch)
        {
            if (batch == null) throw new ArgumentNullException("batch");

            BatchStatement batchStatement = await _statementFactory.GetBatchStatementAsync(batch.Statements);
            await _session.ExecuteAsync(batchStatement);
        }

        public TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value)
        {
            return _mapperFactory.TypeConverter.ConvertCqlArgument<TValue, TDatabase>(value);
        }

        public List<T> Fetch<T>(CqlQueryOptions queryOptions = null)
        {
            // Just let the SQL be auto-generated
            return Fetch<T>(Cql.New(string.Empty, new object[0], queryOptions ?? CqlQueryOptions.None));
        }

        public List<T> Fetch<T>(string cql, params object[] args)
        {
            return Fetch<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public List<T> Fetch<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = _statementFactory.GetStatement(cql);
            RowSet rows = _session.Execute(statement);

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return rows.Select(mapper).ToList();
        }

        public T Single<T>(string cql, params object[] args)
        {
            return Single<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T Single<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = _statementFactory.GetStatement(cql);
            RowSet rows = _session.Execute(statement);

            Row row = rows.Single();

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public T SingleOrDefault<T>(string cql, params object[] args)
        {
            return SingleOrDefault<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T SingleOrDefault<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = _statementFactory.GetStatement(cql);
            RowSet rows = _session.Execute(statement);

            Row row = rows.SingleOrDefault();

            // Map to return type or return default
            if (row == null)
                return default(T);

            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public T First<T>(string cql, params object[] args)
        {
            return First<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T First<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = _statementFactory.GetStatement(cql);
            RowSet rows = _session.Execute(statement);

            Row row = rows.First();

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public T FirstOrDefault<T>(string cql, params object[] args)
        {
            return FirstOrDefault<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T FirstOrDefault<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            Statement statement = _statementFactory.GetStatement(cql);
            RowSet rows = _session.Execute(statement);

            Row row = rows.FirstOrDefault();

            // Map to return type or return default
            if (row == null)
                return default(T);

            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql.Statement, rows);
            return mapper(row);
        }

        public void Insert<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateInsert<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            Execute(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public void Update<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateUpdate<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            Execute(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public void Update<T>(string cql, params object[] args)
        {
            Update<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Update<T>(Cql cql)
        {
            _cqlGenerator.PrependUpdate<T>(cql);
            Execute(cql);
        }

        public void Delete<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get the statement and bind values from POCO
            string cql = _cqlGenerator.GenerateDelete<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesOnly: true);
            object[] values = getBindValues(poco);

            Execute(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public void Delete<T>(string cql, params object[] args)
        {
            Delete<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Delete<T>(Cql cql)
        {
            _cqlGenerator.PrependDelete<T>(cql);
            Execute(cql);
        }

        public void Execute(string cql, params object[] args)
        {
            Execute(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Execute(Cql cql)
        {
            Statement statement = _statementFactory.GetStatement(cql);
            _session.Execute(statement);
        }
    }
}