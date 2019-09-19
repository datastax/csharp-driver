//
//       Copyright (C) 2019 DataStax Inc.
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

using System;

using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Internal;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    internal class RequestObserver : IRequestObserver
    {
        private static readonly Logger Logger = new Logger(typeof(RequestObserver));

        private readonly IMetricsManager _manager;
        private readonly IDriverTimer _requestTimer;
        private IDriverTimeHandler _driverTimeHandler;

        public RequestObserver(IMetricsManager manager, IDriverTimer requestTimer)
        {
            _manager = manager;
            _requestTimer = requestTimer;
        }
        
        public void OnSpeculativeExecution(Host host, long delay)
        {
            _manager.GetOrCreateNodeMetrics(host).SpeculativeExecutions.Increment(1);
        }

        public void OnRequestRetry(Host host, RetryReasonType reason, RetryDecision.RetryDecisionType decision)
        {
            var nodeMetrics = _manager.GetOrCreateNodeMetrics(host);
            OnRequestRetry(nodeMetrics.Errors, reason);
            switch (decision)
            {
                case RetryDecision.RetryDecisionType.Retry:
                    OnRequestRetry(nodeMetrics.Retries, reason);
                    break;

                case RetryDecision.RetryDecisionType.Ignore:
                    OnRequestRetry(nodeMetrics.Ignores, reason);
                    break;
            }
        }

        private void OnRequestRetry(IRequestMetrics metricsRegistry, RetryReasonType reason)
        {
            // todo(sivukhin, 08.08.2019): Missed OnAborted metric?
            metricsRegistry.Total.Mark();
            switch (reason)
            {
                case RetryReasonType.Unavailable:
                    metricsRegistry.Unavailable.Mark();
                    break;

                case RetryReasonType.ReadTimeOut:
                    metricsRegistry.ReadTimeout.Mark();
                    break;

                case RetryReasonType.WriteTimeOut:
                    metricsRegistry.WriteTimeout.Mark();
                    break;

                case RetryReasonType.RequestError:
                case RetryReasonType.Unknown:
                    metricsRegistry.Other.Mark();
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(reason), reason, null);
            }
        }

        public void OnRequestStart()
        {
            try
            {
                _driverTimeHandler = _requestTimer.StartRecording();
            }
            catch (Exception ex)
            {
                LogError(ex);
                _driverTimeHandler = NullDriverTimeHandler.Instance;
            }
        }

        public void OnRequestFinish(Exception exception)
        {
            try
            {
                _driverTimeHandler.EndRecording();
                _driverTimeHandler = null;
            }
            catch (Exception ex)
            {
                LogError(ex);
            }
        }

        private static void LogError(Exception ex)
        {
            Logger.Warning("An error occured while recording metrics for a request. Exception = {0}", ex.ToString());
        }
    }
}