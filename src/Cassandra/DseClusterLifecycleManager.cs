// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Threading;
using System.Threading.Tasks;
using Cassandra.SessionManagement;

namespace Cassandra
{
    internal class DseClusterLifecycleManager : IClusterLifecycleManager
    {
        private static readonly Logger Logger = new Logger(typeof(DseCluster));

        private readonly IInternalDseCluster _dseCluster;

        public DseClusterLifecycleManager(IInternalDseCluster dseCluster)
        {
            _dseCluster = dseCluster;
        }

        public async Task InitializeAsync()
        {
            if (await _dseCluster.OnInitializeAsync().ConfigureAwait(false))
            {
                DseClusterLifecycleManager.Logger.Info("DseCluster [" + _dseCluster.Metadata.ClusterName + "] has been initialized.");
            }
        }

        public async Task ShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            if (await _dseCluster.OnShutdownAsync(timeoutMs).ConfigureAwait(false))
            {
                DseClusterLifecycleManager.Logger.Info("DseCluster [" + _dseCluster.Metadata.ClusterName + "] has been shut down.");
            }
        }
    }
}