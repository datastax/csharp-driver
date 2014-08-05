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
        private readonly CqlStringGenerator _cqlGenerator;

        public CqlClient(ISession session, MapperFactory mapperFactory, StatementFactory statementFactory, CqlStringGenerator cqlGenerator)
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

        public Task<List<T>> Fetch<T>()
        {
            // Just pass an empty string for the CQL and let if be auto-generated
            return Fetch<T>(string.Empty);
        }

        public async Task<List<T>> Fetch<T>(string cql, params object[] args)
        {
            // Get the statement to execute and execute it
            cql = _cqlGenerator.AddSelect<T>(cql);
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);
            
            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql, rows);
            return rows.Select(mapper).ToList();
        }
        
        public async Task<T> Single<T>(string cql, params object[] args)
        {
            // Get the statement to execute and execute it
            cql = _cqlGenerator.AddSelect<T>(cql);
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.Single();

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql, rows);
            return mapper(row);
        }
        
        public async Task<T> SingleOrDefault<T>(string cql, params object[] args)
        {
            // Get the statement to execute and execute it
            cql = _cqlGenerator.AddSelect<T>(cql);
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.SingleOrDefault();

            // Map to return type or return default
            if (row == null) 
                return default(T);

            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql, rows);
            return mapper(row);
        }

        public async Task<T> First<T>(string cql, params object[] args)
        {
            // Get the statement to execute and execute it
            cql = _cqlGenerator.AddSelect<T>(cql);
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.First();

            // Map to return type
            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql, rows);
            return mapper(row);
        }

        public async Task<T> FirstOrDefault<T>(string cql, params object[] args)
        {
            // Get the statement to execute and execute it
            cql = _cqlGenerator.AddSelect<T>(cql);
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            RowSet rows = await _session.ExecuteAsync(statement).ConfigureAwait(false);

            Row row = rows.FirstOrDefault();

            // Map to return type or return default
            if (row == null)
                return default(T);

            Func<Row, T> mapper = _mapperFactory.GetMapper<T>(cql, rows);
            return mapper(row);
        }
        
        public async Task Insert<T>(T poco)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateInsert<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            // Execute the statement
            IStatement statement = await _statementFactory.GetStatement(cql, values).ConfigureAwait(false);
            await _session.ExecuteAsync(statement).ConfigureAwait(false);
        }
        
        public async Task Update<T>(T poco)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateUpdate<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            // Execute
            IStatement statement = await _statementFactory.GetStatement(cql, values).ConfigureAwait(false);
            await _session.ExecuteAsync(statement).ConfigureAwait(false);
        }

        public async Task Update<T>(string cql, params object[] args)
        {
            cql = _cqlGenerator.PrependUpdate<T>(cql);
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            await _session.ExecuteAsync(statement).ConfigureAwait(false);
        }

        public async Task Delete<T>(T poco)
        {
            // Get the statement and bind values from POCO
            string cql = _cqlGenerator.GenerateDelete<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesOnly: true);
            object[] values = getBindValues(poco);

            // Execute
            IStatement statement = await _statementFactory.GetStatement(cql, values).ConfigureAwait(false);
            await _session.ExecuteAsync(statement).ConfigureAwait(false);
        }

        public async Task Delete<T>(string cql, params object[] args)
        {
            cql = _cqlGenerator.PrependDelete<T>(cql);
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            await _session.ExecuteAsync(statement).ConfigureAwait(false);
        }

        public async Task Execute(string cql, params object[] args)
        {
            IStatement statement = await _statementFactory.GetStatement(cql, args).ConfigureAwait(false);
            await _session.ExecuteAsync(statement).ConfigureAwait(false);
        }

        public TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value)
        {
            return _mapperFactory.TypeConverter.ConvertCqlArgument<TValue, TDatabase>(value);
        }
    }
}