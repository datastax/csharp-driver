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

using Cassandra.Observers.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Observers
{
    internal class ObserverFactory : IObserverFactory
    {
        private readonly IInternalSession _session;

        public ObserverFactory(IInternalSession session)
        {
            _session = session;
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new RequestObserver(_session.MetricsManager, _session.MetricsManager.GetSessionMetrics().CqlRequests);
        }

        public IConnectionObserver CreateConnectionObserver(Host host)
        {
            return new ConnectionObserver(_session.MetricsManager.GetSessionMetrics(), _session.MetricsManager.GetOrCreateNodeMetrics(host));
        }

        public IOperationObserver CreateOperationObserver(Host host)
        {
            return new OperationObserver(_session.MetricsManager.GetOrCreateNodeMetrics(host));
        }
    }
}