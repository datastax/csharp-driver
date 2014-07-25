using Cassandra;

namespace CqlPoco.Statements
{
    /// <summary>
    /// A wrapper around C* driver statements.
    /// </summary>
    internal interface IStatementWrapper
    {
        string Cql { get; }
        IStatement Bind(params object[] values);
    }
}