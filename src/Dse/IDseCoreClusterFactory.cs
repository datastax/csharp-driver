// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using Dse.SessionManagement;

namespace Dse
{
    internal interface IDseCoreClusterFactory
    {
        IInternalCluster Create(IInternalDseCluster dseCluster, IInitializer initializer, IReadOnlyList<string> hostnames, Configuration config);
    }
}