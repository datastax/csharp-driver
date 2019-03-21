//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Threading.Tasks;
using Dse.Insights;
using Dse.Tasks;

namespace Dse.SessionManagement
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