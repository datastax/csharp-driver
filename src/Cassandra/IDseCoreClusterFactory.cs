// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using Cassandra.SessionManagement;

namespace Cassandra
{
    internal interface IDseCoreClusterFactory
    {
        IInternalCluster Create(IInternalDseCluster dseCluster, IInitializer initializer, IReadOnlyList<string> hostnames, Configuration config);
    }
}