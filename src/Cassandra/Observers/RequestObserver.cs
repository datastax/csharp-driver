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
using Cassandra.Requests;

namespace Cassandra.Observers
{
    internal class RequestObserver : IRequestObserver
    {
        private static readonly Logger Logger = new Logger(typeof(RequestObserver));

        private readonly IMetricsManager _manager;
        private readonly IDriverTimer _requestTimer;
        private IDriverTimerMeasurement _driverTimerMeasurement;

        public RequestObserver(IMetricsManager manager, IDriverTimer requestTimer)
        {
            _manager = manager;
            _requestTimer = requestTimer;
        }
        
        public void OnSpeculativeExecution(Host host, long delay)
        {
            _manager.GetOrCreateNodeMetrics(host).SpeculativeExecutions.Increment(1);
        }

        public void OnRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision)
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
            metricsRegistry.Total.Increment();
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

        public void OnRequestStart()
        {
            try
            {
                _driverTimerMeasurement = _requestTimer.StartMeasuring();
            }
            catch (Exception ex)
            {
                LogError(ex);
                _driverTimerMeasurement = NullDriverTimerMeasurement.Instance;
            }
        }

        public void OnRequestFinish(Exception exception)
        {
            try
            {
                _driverTimerMeasurement.StopMeasuring();
                _driverTimerMeasurement = null;
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