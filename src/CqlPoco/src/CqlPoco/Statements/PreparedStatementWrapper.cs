using System;
using Cassandra;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Wraps PreparedStatement with a common interface.
    /// </summary>
    internal class PreparedStatementWrapper : IStatementWrapper
    {
        private readonly PreparedStatement _statement;
        private readonly string _cql;

        public string Cql
        {
            get { return _cql; }
        }

        public PreparedStatementWrapper(PreparedStatement statement, string cql)
        {
            if (statement == null) throw new ArgumentNullException("statement");
            if (cql == null) throw new ArgumentNullException("cql");
            _statement = statement;
            _cql = cql;
        }

        public IStatement Bind(params object[] values)
        {
            return _statement.Bind(values);
        }
    }
}