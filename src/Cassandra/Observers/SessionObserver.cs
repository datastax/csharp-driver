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

using Cassandra.Metrics;
using Cassandra.Metrics.Internal;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Observers
{
    internal class SessionObserver : ISessionObserver
    {
        private static readonly Logger Logger = new Logger(typeof(SessionObserver));
        private readonly MetricsManager _metricsManager = MetricsManager.EmptyInstance;
        private Cluster _cluster;
        public SessionMetricsRegistry SessionMetricsRegistry { get; private set; } = SessionMetricsRegistry.EmptyInstance;

        public SessionObserver()
        {
            _metricsManager = new MetricsManager(new NullDriverMetricsProvider(), );
        }

        public SessionObserver(MetricsManager metricsManager)
        {
            _metricsManager = metricsManager;
        }

        public void OnInit(Cluster cluster)
        {
            _cluster = cluster;
            SessionMetricsRegistry = _metricsManager.GetSessionMetrics(cluster);
            SessionMetricsRegistry.InitializeSessionGauges(cluster);
        }

        public void OnConnect(IInternalSession session)
        {
            Logger.Info("Session connected ({0})", session.GetHashCode());
        }

        public void OnShutdown()
        {
            Logger.Info("Cluster [" + _cluster.Metadata.ClusterName + "] has been shut down.");
            // todo(sivukhin, 08.08.2019): Gracefully dispose metrics here
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new RequestObserver(SessionMetricsRegistry.CqlRequests);
        }

        public IHostObserver CreateHostObserver()
        {
            return new HostObserver(this);
        }
    }
}