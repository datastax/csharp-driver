using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Mapping.Statements;

namespace Cassandra.Mapping
{
    /// <summary>
    /// The default CQL client implementation which uses the DataStax driver ISession provided in the constructor
    /// for running queries against a Cassandra cluster.
    /// </summary>
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
            if (session == null) throw new ArgumentNullException("session");
            if (mapperFactory == null) throw new ArgumentNullException("mapperFactory");
            if (statementFactory == null) throw new ArgumentNullException("statementFactory");
            if (cqlGenerator == null) throw new ArgumentNullException("cqlGenerator");

            _session = session;
            _mapperFactory = mapperFactory;
            _statementFactory = statementFactory;
            _cqlGenerator = cqlGenerator;
            if (session.Cluster != null && session.Cluster.Configuration != null)
            {
                _queryAbortTimeout = session.Cluster.Configuration.ClientOptions.QueryAbortTimeout;
            }
        }

        public Task<IEnumerable<T>> FetchAsync<T>(CqlQueryOptions options = null)
        {
            return FetchAsync<T>(Cql.New(string.Empty, new object[0], options ?? CqlQueryOptions.None));
        }

        public Task<IEnumerable<T>> FetchAsync<T>(string cql, params object[] args)
        {
            return FetchAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public Task<IEnumerable<T>> FetchAsync<T>(Cql cql)
        {
            //Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            return _statementFactory
                .GetStatementAsync(_session, cql)
                .Continue(t1 => _session.ExecuteAsync(t1.Result)
                    .Continue(t2 =>
                    {
                        var rs = t2.Result;
                        var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                        return rs.Select(mapper);
                    })).Unwrap();
        }

        public Task<T> SingleAsync<T>(string cql, params object[] args)
        {
            return SingleAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public Task<T> SingleAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            return _statementFactory.GetStatementAsync(_session, cql)
                .Continue(t1 => _session.ExecuteAsync(t1.Result)
                    .Continue(t2 =>
                    {
                        var rs = t2.Result;
                        // Map to return type
                        var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                        return mapper(rs.Single());
                    })).Unwrap();
        }

        public Task<T> SingleOrDefaultAsync<T>(string cql, params object[] args)
        {
            return SingleOrDefaultAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public Task<T> SingleOrDefaultAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            return _statementFactory.GetStatementAsync(_session, cql)
                .Continue(t1 => _session.ExecuteAsync(t1.Result)
                    .Continue(t2 =>
                    {
                        var rs = t2.Result;
                        var row = rs.SingleOrDefault();
                        // Map to return type
                        if (row == null)
                        {
                            return default(T);
                        }
                        var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                        return mapper(row);
                    })).Unwrap();
        }

        public Task<T> FirstAsync<T>(string cql, params object[] args)
        {
            return FirstAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public Task<T> FirstAsync<T>(Cql cql)
        {
            // Get the statement to execute and execute it
            _cqlGenerator.AddSelect<T>(cql);
            return _statementFactory.GetStatementAsync(_session, cql)
                .Continue(t1 => _session.ExecuteAsync(t1.Result)
                    .Continue(t2 =>
                    {
                        var rs = t2.Result;
                        var row = rs.First();
                        // Map to return type
                        var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                        return mapper(row);
                    })).Unwrap();
        }

        public Task<T> FirstOrDefaultAsync<T>(string cql, params object[] args)
        {
            return FirstOrDefaultAsync<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public Task<T> FirstOrDefaultAsync<T>(Cql cql)
        {

            _cqlGenerator.AddSelect<T>(cql);
            return _statementFactory.GetStatementAsync(_session, cql)
                .Continue(t1 => _session.ExecuteAsync(t1.Result)
                    .Continue(t2 =>
                    {
                        var rs = t2.Result;
                        var row = rs.FirstOrDefault();
                        // Map to return type
                        if (row == null)
                        {
                            return default(T);
                        }
                        var mapper = _mapperFactory.GetMapper<T>(cql.Statement, rs);
                        return mapper(row);
                    })).Unwrap();
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
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesLast: true);
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

        public Task ExecuteAsync(Cql cql)
        {
            // Execute the statement
            return _statementFactory
                .GetStatementAsync(_session, cql)
                .Continue(t =>
                {
                    var statement = t.Result;
                    return _session.ExecuteAsync(statement);
                })
                .Unwrap();
        }

        public ICqlBatch CreateBatch()
        {
            return new CqlBatch(_mapperFactory, _cqlGenerator);
        }

        public void Execute(ICqlBatch batch)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(ExecuteAsync(batch), _queryAbortTimeout);
        }

        public Task ExecuteAsync(ICqlBatch batch)
        {
            if (batch == null) throw new ArgumentNullException("batch");

            return _statementFactory
                .GetBatchStatementAsync(_session, batch.Statements)
                .Continue(t =>
                {
                    var batchStatement = t.Result;
                    return _session.ExecuteAsync(batchStatement);
                })
                .Unwrap();
        }

        public TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value)
        {
            return _mapperFactory.TypeConverter.ConvertCqlArgument<TValue, TDatabase>(value);
        }

        public IEnumerable<T> Fetch<T>(CqlQueryOptions queryOptions = null)
        {
            // Just let the SQL be auto-generated
            return Fetch<T>(Cql.New(string.Empty, new object[0], queryOptions ?? CqlQueryOptions.None));
        }

        public IEnumerable<T> Fetch<T>(string cql, params object[] args)
        {
            return Fetch<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public IEnumerable<T> Fetch<T>(Cql cql)
        {
            //Use the async method
            var t = FetchAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        public T Single<T>(string cql, params object[] args)
        {
            return Single<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T Single<T>(Cql cql)
        {
            //Use the async method
            var t = SingleAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        public T SingleOrDefault<T>(string cql, params object[] args)
        {
            return SingleOrDefault<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T SingleOrDefault<T>(Cql cql)
        {
            //Use async method
            var t = SingleOrDefaultAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        public T First<T>(string cql, params object[] args)
        {
            return First<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T First<T>(Cql cql)
        {
            //Use async method
            var t = FirstAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        public T FirstOrDefault<T>(string cql, params object[] args)
        {
            return FirstOrDefault<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public T FirstOrDefault<T>(Cql cql)
        {
            //Use async method
            var t = FirstOrDefaultAsync<T>(cql);
            //Wait for it to be completed or throw
            TaskHelper.WaitToComplete(t, _queryAbortTimeout);
            return t.Result;
        }

        public void Insert<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(InsertAsync(poco, queryOptions), _queryAbortTimeout);
        }

        public void Update<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(UpdateAsync(poco, queryOptions), _queryAbortTimeout);
        }

        public void Update<T>(string cql, params object[] args)
        {
            Update<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Update<T>(Cql cql)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(UpdateAsync(cql), _queryAbortTimeout);
        }

        public void Delete<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(DeleteAsync(poco, queryOptions), _queryAbortTimeout);
        }

        public void Delete<T>(string cql, params object[] args)
        {
            Delete<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Delete<T>(Cql cql)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(DeleteAsync(cql), _queryAbortTimeout);
        }

        public void Execute(string cql, params object[] args)
        {
            Execute(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Execute(Cql cql)
        {
            //Wait async method to be completed or throw
            TaskHelper.WaitToComplete(ExecuteAsync(cql), _queryAbortTimeout);
        }
    }
}