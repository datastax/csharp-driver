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
//

using System;
using System.Diagnostics.Metrics;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.OpenTelemetry.Metrics
{
    internal sealed class DriverGauge : IDriverGauge
    {
#pragma warning disable IDE0052 // Remove unread private members
        private readonly ObservableGauge<double> _gauge;
#pragma warning restore IDE0052 // Remove unread private members

        public DriverGauge(string name, Func<double> value)
        {
            _gauge = CassandraMeter.Instance.CreateObservableGauge(name, value);
        }
    }
}
