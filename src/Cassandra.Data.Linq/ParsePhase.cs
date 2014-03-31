namespace Cassandra.Data.Linq
{
    internal enum ParsePhase
    {
        None,
        Select,
        What,
        Condition,
        SelectBinding,
        Take,
        OrderBy,
        OrderByDescending
    };
}