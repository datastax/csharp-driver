using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    public enum ConsistencyLevel
    {
        Any = 0x0000,
        One = 0x0001,
        Two = 0x0002,
        Three = 0x0003,
        Quorum = 0x0004,
        All = 0x0005,
        LocalQuorum = 0x0006,
        EachQuorum = 0x0007,
        Default = One,
        Ignore = Any
    }
}
