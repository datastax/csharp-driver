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

using App.Metrics.Histogram;

namespace Cassandra.AppMetrics.Implementations
{
    internal class AppMetricsHistogramValue : IAppMetricsHistogramValue
    {
        private readonly HistogramValue _histogram;

        public AppMetricsHistogramValue(HistogramValue histogram)
        {
            _histogram = histogram;
        }

        public long Count => _histogram.Count;

        public double Sum => _histogram.Sum;

        public double LastValue => _histogram.LastValue;

        public double Max => _histogram.Max;

        public double Mean => _histogram.Mean;

        public double Median => _histogram.Median;

        public double Min => _histogram.Min;

        public double Percentile75 => _histogram.Percentile75;

        public double Percentile95 => _histogram.Percentile95;

        public double Percentile98 => _histogram.Percentile98;

        public double Percentile99 => _histogram.Percentile99;

        public double Percentile999 => _histogram.Percentile999;

        public int SampleSize => _histogram.SampleSize;

        public double StdDev => _histogram.StdDev;
    }
}