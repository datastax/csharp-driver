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

namespace Dse.AppMetrics.MetricValues
{
    /// <summary>
    /// Histogram value based on <see cref="HistogramValue"/>.
    /// </summary>
    public interface IAppMetricsHistogramValue
    {
        /// <summary>
        /// Count of all values recorded in this histogram value.
        /// </summary>
        long Count { get; }

        /// <summary>
        /// Sum of all values recorded in this histogram.
        /// </summary>
        double Sum { get; }
        
        /// <summary>
        /// Last value that was recorded in this histogram.
        /// </summary>
        double LastValue { get; }

        /// <summary>
        /// Maximum value.
        /// </summary>
        double Max { get; }

        /// <summary>
        /// Mean.
        /// </summary>
        double Mean { get; }

        /// <summary>
        /// Median.
        /// </summary>
        double Median { get; }

        /// <summary>
        /// Minimum value.
        /// </summary>
        double Min { get; }
        
        /// <summary>
        /// 75th Percentile.
        /// </summary>
        double Percentile75 { get; }
        
        /// <summary>
        /// 95th Percentile.
        /// </summary>
        double Percentile95 { get; }
        
        /// <summary>
        /// 98th Percentile.
        /// </summary>
        double Percentile98 { get; }
        
        /// <summary>
        /// 99th Percentile.
        /// </summary>
        double Percentile99 { get; }
        
        /// <summary>
        /// 99.9th Percentile.
        /// </summary>
        double Percentile999 { get; }

        /// <summary>
        /// Sample size.
        /// </summary>
        int SampleSize { get; }

        /// <summary>
        /// Standard deviation.
        /// </summary>
        double StdDev { get; }
    }
}