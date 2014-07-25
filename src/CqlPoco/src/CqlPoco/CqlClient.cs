using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using CqlPoco.Statements;

namespace CqlPoco
{
    /// <summary>
    /// The default CQL client implementation which uses the DataStax driver ISession provided in the constructor
    /// for running queries against a Cassandra cluster.
    /// </summary>
    public class CqlClient : ICqlClient
    {
        private readonly ISession _session;

        public CqlClient(ISession session)
        {
            if (session == null) throw new ArgumentNullException("session");
            _session = session;
        }

        public Task<List<T>> Fetch<T>()
        {
            throw new System.NotImplementedException();
        }

        public async Task<List<T>> Fetch<T>(string cql, params object[] args)
        {
            // Get the statement to execute
            PocoData pocoData = PocoData.ForType<T>();
            IStatementWrapper statement = await pocoData.StatementFactory.GetSelect(cql).ConfigureAwait(false);

            // Execute the statement
            RowSet rows = await _session.ExecuteAsync(statement.Bind(args)).ConfigureAwait(false);

            // Map to return type
            Func<Row, T> mapper = pocoData.MapperFactory.GetMapper<T>(statement, rows);
            return rows.Select(mapper).ToList();
        }
        
        public async Task<T> Single<T>(string cql, params object[] args)
        {
            // Get statement to execute
            PocoData pocoData = PocoData.ForType<T>();
            IStatementWrapper statement = await pocoData.StatementFactory.GetSelect(cql).ConfigureAwait(false);

            // Execute and get single row
            RowSet rows = await _session.ExecuteAsync(statement.Bind(args)).ConfigureAwait(false);
            Row row = rows.Single();

            // Map to return type
            Func<Row, T> mapper = pocoData.MapperFactory.GetMapper<T>(statement, rows);
            return mapper(row);
        }
        
        public async Task<T> SingleOrDefault<T>(string cql, params object[] args)
        {
            // Get statement to execute
            PocoData pocoData = PocoData.ForType<T>();
            IStatementWrapper statement = await pocoData.StatementFactory.GetSelect(cql).ConfigureAwait(false);

            // Execute and get single row or default
            RowSet rows = await _session.ExecuteAsync(statement.Bind(args)).ConfigureAwait(false);
            Row row = rows.SingleOrDefault();

            // Map to return type or return default
            if (row == null) 
                return default(T);

            Func<Row, T> mapper = pocoData.MapperFactory.GetMapper<T>(statement, rows);
            return mapper(row);
        }

        public async Task<T> First<T>(string cql, params object[] args)
        {
            // Get statement to execute
            PocoData pocoData = PocoData.ForType<T>();
            IStatementWrapper statement = await pocoData.StatementFactory.GetSelect(cql).ConfigureAwait(false);

            // Execute and get single row or default
            RowSet rows = await _session.ExecuteAsync(statement.Bind(args)).ConfigureAwait(false);
            Row row = rows.First();

            // Map to return type
            Func<Row, T> mapper = pocoData.MapperFactory.GetMapper<T>(statement, rows);
            return mapper(row);
        }

        public async Task<T> FirstOrDefault<T>(string cql, params object[] args)
        {
            // Get statement to execute
            PocoData pocoData = PocoData.ForType<T>();
            IStatementWrapper statement = await pocoData.StatementFactory.GetSelect(cql).ConfigureAwait(false);

            // Execute and get single row or default
            RowSet rows = await _session.ExecuteAsync(statement.Bind(args)).ConfigureAwait(false);
            Row row = rows.FirstOrDefault();

            // Map to return type or return default
            if (row == null)
                return default(T);

            Func<Row, T> mapper = pocoData.MapperFactory.GetMapper<T>(statement, rows);
            return mapper(row);
        }

        public Task Insert<T>(T poco)
        {
            throw new System.NotImplementedException();
        }

        public Task Update<T>(T poco)
        {
            throw new System.NotImplementedException();
        }

        public Task Delete<T>(T poco)
        {
            throw new System.NotImplementedException();
        }
    }
}