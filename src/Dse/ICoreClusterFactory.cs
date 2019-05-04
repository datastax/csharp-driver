//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse
{
    internal interface ICoreClusterFactory
    {
        Cluster Create(IInitializer initializer, IReadOnlyList<string> hostnames, Configuration config, IClusterLifecycleManager lifecycleManager);
    }
}