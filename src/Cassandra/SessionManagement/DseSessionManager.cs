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

using System.Threading.Tasks;
using Cassandra.Insights;
using Cassandra.Tasks;

namespace Cassandra.SessionManagement
{
    internal class DseSessionManager : ISessionManager
    {
        private readonly IInsightsClient _insightsClient;

        public DseSessionManager(IInsightsClient insightsClient)
        {
            _insightsClient = insightsClient;
        }
        
        public Task OnInitializationAsync()
        {
            _insightsClient.Init();
            return TaskHelper.Completed;
        }

        public async Task OnShutdownAsync()
        {
            await _insightsClient.ShutdownAsync().ConfigureAwait(false);
        }
    }
}