//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
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