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

using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    //TODO DELETE
    internal class HostObserver : IHostObserver
    {
        private static readonly Logger Logger = new Logger(typeof(HostObserver));

        private Host _host;
        private INodeMetrics _nodeMetrics;

        public HostObserver(Host host, INodeMetrics nodeMetrics)
        {
            _host = host;
            _nodeMetrics = nodeMetrics;
        }

        public void OnSpeculativeExecution(long delay)
        {
            Logger.Info("Starting new speculative execution after {0}, last used host {1}", delay, _host.Address);
            _nodeMetrics.SpeculativeExecutions.Increment(1);
        }

        public void OnRequestRetry(RetryReasonType reason, RetryDecision.RetryDecisionType decision)
        {
            OnRequestRetry(_nodeMetrics.Errors, reason);
            switch (decision)
            {
                case RetryDecision.RetryDecisionType.Retry:
                    OnRequestRetry(_nodeMetrics.Retries, reason);
                    break;

                case RetryDecision.RetryDecisionType.Ignore:
                    OnRequestRetry(_nodeMetrics.Ignores, reason);
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
    }
}