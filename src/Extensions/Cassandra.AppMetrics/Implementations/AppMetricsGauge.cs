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

using App.Metrics;
using App.Metrics.Gauge;
using Cassandra.AppMetrics.MetricTypes;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics.Implementations
{
    /// <inheritdoc />
    internal class AppMetricsGauge : IAppMetricsGauge
    {
        private readonly IMetrics _metrics;
        private readonly IGauge _gauge;

        public AppMetricsGauge(IMetrics metrics, IGauge gauge, string bucket, string path, DriverMeasurementUnit measurementUnit)
        {
            _metrics = metrics;
            _gauge = gauge;
            Context = bucket;
            Name = path;
            MeasurementUnit = measurementUnit.ToAppMetricsUnit();
        }

        /// <inheritdoc />
        public string Context { get; }

        /// <inheritdoc />
        public string Name { get; }

        /// <inheritdoc />
        public Unit MeasurementUnit { get; }

        /// <inheritdoc />
        public double? GetValue()
        {
            var value = _metrics.Snapshot.GetForContext(Context).Gauges.ValueFor(Name);
            return double.IsNaN(value) ? null : (double?)value;
        }
    }
}