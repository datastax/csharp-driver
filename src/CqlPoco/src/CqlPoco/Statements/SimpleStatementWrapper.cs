using System;
using Cassandra;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Wraps SimpleStatements with a common interface.
    /// </summary>
    internal class SimpleStatementWrapper : IStatementWrapper
    {
        private readonly SimpleStatement _statement;

        public string Cql
        {
            get { return _statement.QueryString; }
        }

        public SimpleStatementWrapper(SimpleStatement statement)
        {
            if (statement == null) throw new ArgumentNullException("statement");
            _statement = statement;
        }

        public IStatement Bind(params object[] values)
        {
            return _statement.Bind(values);
        }
    }
}