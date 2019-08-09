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
using Cassandra.Connections;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    internal class HostObserver : IHostObserver
    {
        private static readonly Logger Logger = new Logger(typeof(HostObserver));
        private readonly ClusterObserver _clusterObserver = new ClusterObserver();

        private Host _host;
        private HostLevelMetricsRegistry _hostLevelMetricsRegistry = HostLevelMetricsRegistry.EmptyInstance;

        public HostLevelMetricsRegistry HostLevelMetricsRegistry
        {
            get
            {
                if (_host == null ||
                    !ReferenceEquals(_hostLevelMetricsRegistry, HostLevelMetricsRegistry.EmptyInstance) ||
                    ReferenceEquals(_clusterObserver.ClusterLevelMetricsRegistry, ClusterLevelMetricsRegistry.EmptyInstance))
                    return _hostLevelMetricsRegistry;

                return _hostLevelMetricsRegistry = _clusterObserver.ClusterLevelMetricsRegistry.GetHostLevelMetrics(_host);
            }
        }

        public HostObserver()
        {
        }

        public HostObserver(ClusterObserver clusterObserver)
        {
            _clusterObserver = clusterObserver;
        }

        public void OnSpeculativeExecution(long delay)
        {
            Logger.Info("Starting new speculative execution after {0}, last used host {1}", delay, _host.Address);
            HostLevelMetricsRegistry.SpeculativeExecutions.Increment(1);
        }

        public IConnectionObserver CreateConnectionObserver()
        {
            return new ConnectionObserver(_clusterObserver, this);
        }

        public void OnHostInit(Host host)
        {
            _host = host;
        }

        public void OnHostConnectionPoolInit(HostConnectionPool hostConnectionPool)
        {
            HostLevelMetricsRegistry.InitializeHostConnectionPoolGauges(hostConnectionPool);
        }

        public void OnRequestRetry(RetryReasonType reason, RetryDecision.RetryDecisionType decision)
        {
            OnRequestRetry(HostLevelMetricsRegistry.Errors, reason);
            switch (decision)
            {
                case RetryDecision.RetryDecisionType.Retry:
                    OnRequestRetry(HostLevelMetricsRegistry.Retries, reason);
                    break;
                case RetryDecision.RetryDecisionType.Ignore:
                    OnRequestRetry(HostLevelMetricsRegistry.Ignores, reason);
                    break;
            }
        }

        private void OnRequestRetry(RequestErrorsLevelMetricsRegistry metricsRegistry, RetryReasonType reason)
        {
            // todo(sivukhin, 08.08.2019): Missed OnAborted metric?
            metricsRegistry.Total.Mark();
            switch (reason)
            {
                case RetryReasonType.Unavailable:
                    metricsRegistry.OnUnavailable.Mark();
                    break;
                case RetryReasonType.ReadTimeOut:
                    metricsRegistry.OnReadTimeout.Mark();
                    break;
                case RetryReasonType.WriteTimeOut:
                    metricsRegistry.OnWriteTimeout.Mark();
                    break;
                case RetryReasonType.RequestError:
                case RetryReasonType.Unknown:
                    metricsRegistry.OnOtherError.Mark();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(reason), reason, null);
            }
        }
    }
}