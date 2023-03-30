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

using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.DataStax.Insights.MessageFactories;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.DataStax.Insights.Schema.StatusMessage;
using Cassandra.Responses;
using Cassandra.SessionManagement;
using Cassandra.Tasks;
using Newtonsoft.Json;

namespace Cassandra.DataStax.Insights
{
    internal class InsightsClient : IInsightsClient
    {
        private const string ReportInsightRpc = "CALL InsightsRpc.reportInsight(?)";
        private const int ErrorCountThresholdForLogging = 5;

        private static readonly Logger Logger = new Logger(typeof(InsightsClient));

        private readonly IInternalCluster _cluster;
        private readonly IInternalSession _session;
        private readonly MonitorReportingOptions _monitorReportingOptions;
        private readonly IInsightsMessageFactory<InsightsStartupData> _startupMessageFactory;
        private readonly IInsightsMessageFactory<InsightsStatusData> _statusMessageFactory;

        private Task _insightsTask = null;
        private CancellationTokenSource _cancellationTokenSource;
        private volatile int _errorCount = 0;

        public InsightsClient(
            IInternalCluster cluster,
            IInternalSession session,
            IInsightsMessageFactory<InsightsStartupData> startupMessageFactory,
            IInsightsMessageFactory<InsightsStatusData> statusMessageFactory)
        {
            _cluster = cluster;
            _session = session;
            _monitorReportingOptions = cluster.Configuration.MonitorReportingOptions;
            _startupMessageFactory = startupMessageFactory;
            _statusMessageFactory = statusMessageFactory;
        }

        private bool Initialized => _insightsTask != null;

        private async Task<bool> SendStartupMessageAsync()
        {
            try
            {
                await SendJsonMessageAsync(_startupMessageFactory.CreateMessage(_cluster, _session)).ConfigureAwait(false);
                _errorCount = 0;
                return true;
            }
            catch (Exception ex)
            {
                if (_cancellationTokenSource.IsCancellationRequested || 
                    _errorCount >= InsightsClient.ErrorCountThresholdForLogging)
                {
                    return false;
                }

                _errorCount++;
                InsightsClient.Logger.Verbose("Could not send insights startup event. Exception: {0}", ex.ToString());
                return false;
            }
        }

        private async Task<bool> SendStatusMessageAsync()
        {
            try
            {
                await SendJsonMessageAsync(_statusMessageFactory.CreateMessage(_cluster, _session)).ConfigureAwait(false);
                _errorCount = 0;
                return true;
            }
            catch (Exception ex)
            {
                if (_cancellationTokenSource.IsCancellationRequested || 
                    _errorCount >= InsightsClient.ErrorCountThresholdForLogging)
                {
                    return false;
                }

                _errorCount++;
                InsightsClient.Logger.Verbose("Could not send insights status event. Exception: {0}", ex.ToString());
                return false;
            }
        }

        public void Init()
        {
            if (!ShouldStartInsightsTask())
            {
                _insightsTask = null;
                return;
            }

            _cancellationTokenSource = new CancellationTokenSource();
            _insightsTask = Task.Run(MainLoopAsync);
        }

        public Task ShutdownAsync()
        {
            if (!Initialized)
            {
                return TaskHelper.Completed;
            }

            _cancellationTokenSource.Cancel();
            return _insightsTask;
        }

        public void Dispose()
        {
            ShutdownAsync().GetAwaiter().GetResult();
        }

        private bool ShouldStartInsightsTask()
        {
            return _monitorReportingOptions.MonitorReportingEnabled && _cluster.Configuration.InsightsSupportVerifier.SupportsInsights(_cluster);
        }

        private async Task MainLoopAsync()
        {
            try
            {
                var startupSent = false;
                var isFirstDelay = true;

                // The initial delay should contain some random portion
                // Initial delay should be statusEventDelay - (0 to 10%)
                var percentageToSubtract = new Random(Guid.NewGuid().GetHashCode()).NextDouble() * 0.1;
                var delay = _monitorReportingOptions.StatusEventDelayMilliseconds - 
                            (_monitorReportingOptions.StatusEventDelayMilliseconds * percentageToSubtract);
                
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    if (!startupSent)
                    {
                        startupSent = await SendStartupMessageAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        await SendStatusMessageAsync().ConfigureAwait(false);
                    }
                    
                    await TaskHelper.DelayWithCancellation(
                        TimeSpan.FromMilliseconds(delay), _cancellationTokenSource.Token).ConfigureAwait(false);

                    if (isFirstDelay)
                    {
                        isFirstDelay = false;
                        delay = _monitorReportingOptions.StatusEventDelayMilliseconds;
                    }
                }
            }
            catch (Exception ex)
            {
                InsightsClient.Logger.Verbose("Unhandled exception in Insights task. Insights metrics will not be sent to the server anymore.", ex);
            }

            InsightsClient.Logger.Verbose("Insights task is ending.");
        }

        private async Task SendJsonMessageAsync<T>(T message)
        {
            var queryProtocolOptions = new QueryProtocolOptions(
                ConsistencyLevel.One,
                new object[] { JsonConvert.SerializeObject(message, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore }) },
                false,
                0,
                null,
                ConsistencyLevel.Any);

            var response = await RunWithTokenAsync(() => 
                _cluster.Metadata.ControlConnection.UnsafeSendQueryRequestAsync(
                    InsightsClient.ReportInsightRpc, 
                    queryProtocolOptions)).ConfigureAwait(false);

            if (response == null)
            {
                throw new DriverInternalError("Received null response.");
            }

            if (!(response is ResultResponse resultResponse))
            {
                throw new DriverInternalError("Expected ResultResponse but received: " + response.GetType());
            }

            if (resultResponse.Kind != ResultResponse.ResultResponseKind.Void)
            {
                throw new DriverInternalError("Expected ResultResponse of Kind \"Void\" but received: " + resultResponse.Kind);
            }
        }

        private Task<Response> RunWithTokenAsync(Func<Task<Response>> func)
        {
            var task = Task.Run(func, _cancellationTokenSource.Token);

            // (A canceled task will raise an exception when awaited).
            return task;
        }
    }
}