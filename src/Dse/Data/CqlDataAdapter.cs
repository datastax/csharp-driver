//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

#if !NETCORE
using System.Data.Common;

namespace Cassandra.Data
{
    /// <summary>
    /// Implementation of the <see cref="System.Data.IDbDataAdapter"/> interface. Provides
    /// strong typing, but inherit most of the functionality needed to fully implement a DataAdapter.
    /// </summary>
    /// <inheritdoc />
    public class CqlDataAdapter : DbDataAdapter
    {
    }
}
#endif