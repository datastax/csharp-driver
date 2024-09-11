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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Internal;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;
using Cassandra.Tasks;

namespace Cassandra.Observers.Metrics
{
    internal class MetricsRequestObserver : IRequestObserver
    {
        private static readonly Logger Logger = new Logger(typeof(MetricsRequestObserver));
        private static readonly long Factor = 1000L * 1000L * 1000L / Stopwatch.Frequency;

        private readonly IMetricsManager _manager;
        private readonly IDriverTimer _requestTimer;
        private long _startTimestamp;

        public MetricsRequestObserver(IMetricsManager manager, IDriverTimer requestTimer)
        {
            _manager = manager;
            _requestTimer = requestTimer;
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
            _manager.GetOrCreateNodeMetrics(host).SpeculativeExecutions.Increment(1);
        }

        public Task OnNodeRequestErrorAsync(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision, RequestTrackingInfo r, Exception ex)
        {
            var nodeMetrics = _manager.GetOrCreateNodeMetrics(host);
            OnRequestError(nodeMetrics.Errors, errorType);
            switch (decision)
            {
                case RetryDecision.RetryDecisionType.Retry:
                    OnRetryPolicyDecision(nodeMetrics.Retries, errorType);
                    break;

                case RetryDecision.RetryDecisionType.Ignore:
                    OnRetryPolicyDecision(nodeMetrics.Ignores, errorType);
                    break;
            }

            return TaskHelper.Completed;
        }

        private void OnRetryPolicyDecision(IRetryPolicyMetrics metricsRegistry, RequestErrorType reason)
        {
            metricsRegistry.Total.Increment();
            switch (reason)
            {
                case RequestErrorType.Unavailable:
                    metricsRegistry.Unavailable.Increment();
                    break;

                case RequestErrorType.ReadTimeOut:
                    metricsRegistry.ReadTimeout.Increment();
                    break;

                case RequestErrorType.WriteTimeOut:
                    metricsRegistry.WriteTimeout.Increment();
                    break;

                case RequestErrorType.Other:
                case RequestErrorType.Aborted:
                case RequestErrorType.Unsent:
                case RequestErrorType.ClientTimeout:
                    metricsRegistry.Other.Increment();
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(reason), reason, null);
            }
        }

        private void OnRequestError(IRequestErrorMetrics metricsRegistry, RequestErrorType errorType)
        {
            switch (errorType)
            {
                case RequestErrorType.Unavailable:
                    metricsRegistry.Unavailable.Increment();
                    break;

                case RequestErrorType.ReadTimeOut:
                    metricsRegistry.ReadTimeout.Increment();
                    break;

                case RequestErrorType.WriteTimeOut:
                    metricsRegistry.WriteTimeout.Increment();
                    break;

                case RequestErrorType.Other:
                    metricsRegistry.Other.Increment();
                    break;

                case RequestErrorType.Aborted:
                    metricsRegistry.Aborted.Increment();
                    break;

                case RequestErrorType.Unsent:
                    metricsRegistry.Unsent.Increment();
                    break;

                case RequestErrorType.ClientTimeout:
                    metricsRegistry.ClientTimeout.Increment();
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(errorType), errorType, null);
            }
        }

        public Task OnRequestStartAsync(RequestTrackingInfo r)
        {
            if (!_manager.AreSessionTimerMetricsEnabled)
            {
                return TaskHelper.Completed;
            }

            Interlocked.Exchange(ref _startTimestamp, Stopwatch.GetTimestamp());

            return TaskHelper.Completed;
        }

        public Task OnRequestFailureAsync(Exception ex, RequestTrackingInfo r)
        {
            return OnRequestFinish(ex, r);
        }

        public Task OnRequestSuccessAsync(RequestTrackingInfo r)
        {
            return OnRequestFinish(null, r);
        }

        private Task OnRequestFinish(Exception ex, RequestTrackingInfo r)
        {
            if (!_manager.AreSessionTimerMetricsEnabled)
            {
                return TaskHelper.Completed;
            }

            try
            {
                var startTimestamp = Interlocked.Read(ref _startTimestamp);
                if (startTimestamp == 0)
                {
                    Logger.Warning("Start timestamp wasn't recorded, discarding this measurement.");
                    return TaskHelper.Completed;
                }

                _requestTimer.Record((Stopwatch.GetTimestamp() - startTimestamp) * Factor);
            }
            catch (Exception exception)
            {
                LogError(exception);
            }

            return TaskHelper.Completed;
        }

        private static void LogError(Exception ex)
        {
            Logger.Warning("An error occured while recording metrics for a request. Exception = {0}", ex.ToString());
        }

        public Task OnNodeStartAsync(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            return TaskHelper.Completed;
        }

        public Task OnNodeSuccessAsync(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            return TaskHelper.Completed;
        }
    }
}