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

using Cassandra.DataStax.Insights.MessageFactories;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.DataStax.Insights.Schema.StatusMessage;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights
{
    internal class InsightsClientFactory : IInsightsClientFactory
    {
        private readonly IInsightsMessageFactory<InsightsStartupData> _startupMessageFactory;
        private readonly IInsightsMessageFactory<InsightsStatusData> _statusMessageFactory;

        public InsightsClientFactory(
            IInsightsMessageFactory<InsightsStartupData> startupMessageFactory,
            IInsightsMessageFactory<InsightsStatusData> statusMessageFactory)
        {
            _startupMessageFactory = startupMessageFactory;
            _statusMessageFactory = statusMessageFactory;
        }

        public IInsightsClient Create(IInternalCluster cluster, IInternalSession session)
        {
            return new InsightsClient(cluster, session, _startupMessageFactory, _statusMessageFactory);
        }
    }
}