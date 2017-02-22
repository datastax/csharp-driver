//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

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