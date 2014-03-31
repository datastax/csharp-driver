using System;

namespace Cassandra.Data.Linq
{
    [Flags]
    public enum TableType
    {
        Standard = 0x1,
        Counter = 0x2,
        All = Standard | Counter
    }
}