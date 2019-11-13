//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
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