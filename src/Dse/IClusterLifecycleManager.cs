//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Threading;
using System.Threading.Tasks;

namespace Dse
{
    internal interface IClusterLifecycleManager
    {
        Task InitializeAsync();

        Task ShutdownAsync(int timeoutMs = Timeout.Infinite);
    }
}