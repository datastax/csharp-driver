// Copyright (c) Allan Hardy. All rights reserved.
// Licensed under the Apache License, Version 2.0.
//
// See LICENSE in the project root for license information:
// https://github.com/alhardy/AppMetrics.Reservoirs/blob/3a1861386ac73b21b37e548c61575602eed67f89/LICENSE

using System.Collections.Generic;
using System.Linq;

using App.Metrics.ReservoirSampling;

using Cassandra.AppMetrics.HdrHistogram;

namespace Cassandra.AppMetrics.Implementations
{
    /// <summary>
    /// Adapted code from
    /// https://github.com/alhardy/AppMetrics.Reservoirs/tree/3a1861386ac73b21b37e548c61575602eed67f89/src/App.Metrics.Extensions.Reservoirs.HdrHistogram
    /// </summary>
    internal sealed class HdrSnapshot : IReservoirSnapshot
    {
        private readonly HistogramBase _histogram;

        /// <summary>
        ///     Initializes a new instance of the <see cref="HdrSnapshot" /> class.
        /// </summary>
        /// <param name="histogram">The histogram.</param>
        /// <param name="minValue">The minimum value.</param>
        /// <param name="minUserValue">The minimum user value.</param>
        /// <param name="maxValue">The maximum value.</param>
        /// <param name="maxUserValue">The maximum user value.</param>
        public HdrSnapshot(HistogramBase histogram, long minValue, string minUserValue, long maxValue, string maxUserValue)
        {
            _histogram = histogram;
            Min = !string.IsNullOrWhiteSpace(minUserValue)
                ? minValue
                : histogram.HighestEquivalentValue(histogram.RecordedValues().Select(hiv => hiv.ValueIteratedTo).FirstOrDefault());
            MinUserValue = minUserValue;
            Max = !string.IsNullOrWhiteSpace(maxUserValue) ? maxValue : _histogram.GetMaxValue();
            MaxUserValue = maxUserValue;
        }

        /// <inheritdoc cref="IReservoirSnapshot" />
        public long Count => _histogram.TotalCount;

        /// <inheritdoc cref="IReservoirSnapshot" />
        public long Max { get; }

        /// <inheritdoc cref="IReservoirSnapshot" />
        public string MaxUserValue { get; }

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double Mean => _histogram.GetMean();

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double Median => _histogram.GetValueAtPercentile(50);

        /// <inheritdoc cref="IReservoirSnapshot" />
        public long Min { get; }

        /// <inheritdoc cref="IReservoirSnapshot" />
        public string MinUserValue { get; }

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double Percentile75 => _histogram.GetValueAtPercentile(75);

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double Percentile95 => _histogram.GetValueAtPercentile(95);

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double Percentile98 => _histogram.GetValueAtPercentile(98);

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double Percentile99 => _histogram.GetValueAtPercentile(99);

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double Percentile999 => _histogram.GetValueAtPercentile(99.9);

        /// <inheritdoc cref="IReservoirSnapshot" />
        public int Size => _histogram.GetEstimatedFootprintInBytes();

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double StdDev => _histogram.GetStdDeviation();

        /// <inheritdoc />
        public double Sum => Values.Sum();

        /// <inheritdoc cref="IReservoirSnapshot" />
        public IEnumerable<long> Values
        {
            get { return _histogram.RecordedValues().Select(v => v.ValueIteratedTo); }
        }

        /// <inheritdoc cref="IReservoirSnapshot" />
        public double GetValue(double quantile) { return _histogram.GetValueAtPercentile(quantile * 100); }
    }
}