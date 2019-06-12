// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Threading;
using System.Threading.Tasks;

using Cassandra.SessionManagement;

namespace Cassandra
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