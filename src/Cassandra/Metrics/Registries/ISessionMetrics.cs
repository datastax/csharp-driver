//
//       Copyright (C) DataStax Inc.
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

using System;
using Cassandra.Metrics.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Registries
{
    internal interface ISessionMetrics : IMetricsRegistry, IDisposable
    {
        IDriverTimer CqlRequests { get; }

        IDriverMeter CqlClientTimeouts { get; }

        IDriverCounter BytesSent { get; }

        IDriverCounter BytesReceived { get; }

        IDriverGauge ConnectedNodes { get; }

        void InitializeMetrics(IInternalSession session);
    }
}