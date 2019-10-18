// Copyright (c) Allan Hardy. All rights reserved.
// Licensed under the Apache License, Version 2.0.
//
// See LICENSE in the project root for license information:
// https://github.com/alhardy/AppMetrics.Reservoirs/blob/3a1861386ac73b21b37e548c61575602eed67f89/LICENSE

using App.Metrics.Concurrency;
using App.Metrics.ReservoirSampling;

using Cassandra.AppMetrics.HdrHistogram;

namespace Cassandra.AppMetrics.Implementations
{
    /// <summary>
    /// Adapted code from
    /// https://github.com/alhardy/AppMetrics.Reservoirs/tree/3a1861386ac73b21b37e548c61575602eed67f89/src/App.Metrics.Extensions.Reservoirs.HdrHistogram
    /// </summary>
    internal sealed class HdrHistogramReservoir : IReservoir
    {
        private static readonly Logger Logger = new Logger(typeof(HdrHistogramReservoir));

        private readonly long _highestTrackableValue;
        private readonly object _maxValueLock = new object();
        private readonly object _minValueLock = new object();
        private readonly Recorder _recorder;

        private readonly HistogramBase _runningTotals;
        private HistogramBase _intervalHistogram;
        private string _maxUserValue;

        private AtomicLong _maxValue = new AtomicLong(0);
        private string _minUserValue;

        private AtomicLong _minValue = new AtomicLong(long.MaxValue);

        /// <summary>
        ///     Initializes a new instance of the <see cref="HdrHistogramReservoir" /> class.
        /// </summary>
        public HdrHistogramReservoir(long lowestTrackableValue, long highestTrackableValue, int numberOfSignificantValueDigits)
        {
            _highestTrackableValue = highestTrackableValue;

            var recorder = HistogramFactory
                .With64BitBucketSize()
                .WithValuesFrom(lowestTrackableValue)
                .WithValuesUpTo(highestTrackableValue)
                .WithPrecisionOf(numberOfSignificantValueDigits)
                .WithThreadSafeWrites()
                .WithThreadSafeReads()
                .Create();

            _recorder = recorder;

            _intervalHistogram = recorder.GetIntervalHistogram();
            _runningTotals = new LongHistogram(lowestTrackableValue, highestTrackableValue, _intervalHistogram.NumberOfSignificantValueDigits);
        }

        /// <inheritdoc cref="IReservoir" />
        public IReservoirSnapshot GetSnapshot(bool resetReservoir)
        {
            var snapshot = new HdrSnapshot(
                UpdateTotals(),
                _minValue.GetValue(),
                _minUserValue,
                _maxValue.GetValue(),
                _maxUserValue);

            if (resetReservoir)
            {
                Reset();
            }

            return snapshot;
        }

        /// <inheritdoc />
        public IReservoirSnapshot GetSnapshot() { return GetSnapshot(false); }

        /// <inheritdoc cref="IReservoir" />
        public void Reset()
        {
            _recorder.Reset();
            _runningTotals.Reset();
            _intervalHistogram.Reset();
        }

        /// <inheritdoc cref="IReservoir" />
        public void Update(long value, string userValue)
        {
            if (value > _highestTrackableValue)
            {
                HdrHistogramReservoir.Logger.Warning(
                    "Value {0} ns is higher than the configured HighestLatency {1} ns. Discarding this measurement. " +
                    "Consider increasing this limit in DriverAppMetricsOptions.",
                    value,
                    _highestTrackableValue);
                return;
            }

            _recorder.RecordValue(value);
            if (userValue != null)
            {
                TrackMinMaxUserValue(value, userValue);
            }
        }

        /// <inheritdoc />
        public void Update(long value) { Update(value, null); }

        private void SetMaxValue(long value, string userValue)
        {
            long current;
            while (value > (current = _maxValue.GetValue()))
            {
                _maxValue.CompareAndSwap(current, value);
            }

            if (value == current)
            {
                lock (_maxValueLock)
                {
                    if (value == _maxValue.GetValue())
                    {
                        _maxUserValue = userValue;
                    }
                }
            }
        }

        private void SetMinValue(long value, string userValue)
        {
            long current;
            while (value < (current = _minValue.GetValue()))
            {
                _minValue.CompareAndSwap(current, value);
            }

            if (value == current)
            {
                lock (_minValueLock)
                {
                    if (value == _minValue.GetValue())
                    {
                        _minUserValue = userValue;
                    }
                }
            }
        }

        private void TrackMinMaxUserValue(long value, string userValue)
        {
            if (value > _maxValue.NonVolatileGetValue())
            {
                SetMaxValue(value, userValue);
            }

            if (value < _minValue.NonVolatileGetValue())
            {
                SetMinValue(value, userValue);
            }
        }

        private HistogramBase UpdateTotals()
        {
            lock (_runningTotals)
            {
                _intervalHistogram = _recorder.GetIntervalHistogram(_intervalHistogram);
                _runningTotals.Add(_intervalHistogram);
                return _runningTotals.Copy();
            }
        }
    }
}