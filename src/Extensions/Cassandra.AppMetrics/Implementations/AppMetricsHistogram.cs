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
using App.Metrics.Histogram;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics.Implementations
{
    internal class AppMetricsHistogram : IDriverHistogram
    {
        private readonly IMetrics _metrics;
        private readonly IHistogram _histogram;

        public AppMetricsHistogram(
            IMetrics metrics, IHistogram histogram, string bucket, string path, DriverMeasurementUnit measurementUnit)
        {
            _metrics = metrics;
            _histogram = histogram;
            Context = bucket;
            Name = path;
            MeasurementUnit = measurementUnit.ToAppMetricsUnit();
        }

        public void Update(long value)
        {
            _histogram.Update(value);
        }

        public string Context { get; }
        
        public string Name { get; }

        public Unit MeasurementUnit { get; }

        public IAppMetricsHistogramValue GetValue()
        {
            return new AppMetricsHistogramValue(_metrics.Snapshot.GetForContext(Context).Histograms.ValueFor(Name));
        }
    }
}