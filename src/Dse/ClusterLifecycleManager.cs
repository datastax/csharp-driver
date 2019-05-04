//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Threading;
using System.Threading.Tasks;

using Dse.SessionManagement;

namespace Dse
{
    internal class ClusterLifecycleManager : IClusterLifecycleManager
    {
        private static readonly Logger Logger = new Logger(typeof(Cluster));
        private readonly IInternalCluster _cluster;

        public ClusterLifecycleManager(IInternalCluster cluster)
        {
            _cluster = cluster;
        }
        
        public async Task InitializeAsync()
        {
            if (await _cluster.OnInitializeAsync().ConfigureAwait(false))
            {
                ClusterLifecycleManager.Logger.Info("Cluster [" + _cluster.Metadata.ClusterName + "] has been initialized.");
            }
        }

        public async Task ShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            if (await _cluster.OnShutdownAsync(timeoutMs).ConfigureAwait(false))
            {
                ClusterLifecycleManager.Logger.Info("Cluster [" + _cluster.Metadata.ClusterName + "] has been shut down.");
            }
        }
    }
}