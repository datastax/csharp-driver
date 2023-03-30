// Copyright (c) Allan Hardy. All rights reserved.
// Licensed under the Apache License, Version 2.0.
//
// See LICENSE in the project root for license information:
// https://github.com/alhardy/AppMetrics.Reservoirs/blob/3a1861386ac73b21b37e548c61575602eed67f89/LICENSE

using System;
using System.Threading;
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

        private static readonly IReservoirSnapshot _emptySnapshot;

        private readonly long _highestTrackableValue;
        private readonly long _refreshIntervalTicks;
        private readonly object _maxValueLock = new object();
        private readonly object _minValueLock = new object();
        private readonly Recorder _recorder;

        private HistogramBase _intervalHistogram;
        private string _maxUserValue;

        private AtomicLong _maxValue = new AtomicLong(0);
        private string _minUserValue;

        private AtomicLong _minValue = new AtomicLong(long.MaxValue);

        private volatile IReservoirSnapshot _cachedSnapshot;
        private long _lastRefreshTicks;
        private object _refreshLock = new object();

        static HdrHistogramReservoir()
        {
            HdrHistogramReservoir._emptySnapshot = new HdrSnapshot(
                HistogramFactory
                    .With64BitBucketSize()
                    .WithThreadSafeReads()
                    .Create()
                    .GetIntervalHistogram(),
                0,
                null,
                0,
                null);
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="HdrHistogramReservoir" /> class.
        /// </summary>
        public HdrHistogramReservoir(
            long lowestTrackableValue, long highestTrackableValue, int numberOfSignificantValueDigits, long refreshIntervalMilliseconds)
        {
            _highestTrackableValue = highestTrackableValue;
            _refreshIntervalTicks = TimeSpan.FromMilliseconds(refreshIntervalMilliseconds).Ticks;

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
            UnsafeRefresh(DateTime.UtcNow.Ticks, true);
        }

        private bool NeedsRefresh(long now)
        {
            return now - Interlocked.Read(ref _lastRefreshTicks) >= _refreshIntervalTicks;
        }

        private void RefreshIfNeeded()
        {
            var now = DateTime.UtcNow.Ticks;
            if (NeedsRefresh(now))
            {
                lock (_refreshLock)
                {
                    // check again inside the critical section
                    if (NeedsRefresh(now))
                    {
                        UnsafeRefresh(now, false);
                    }
                }
            }
        }

        private void UnsafeRefresh(long nowTicks, bool empty)
        {
            // don't recycle the current histogram as it is being used by the cached snapshot
            _intervalHistogram = _recorder.GetIntervalHistogram();
            Interlocked.Exchange(ref _lastRefreshTicks, nowTicks);
            _cachedSnapshot = BuildSnapshot(empty);
        }

        private IReservoirSnapshot BuildSnapshot(bool empty)
        {
            if (empty)
            {
                return HdrHistogramReservoir._emptySnapshot;
            }

            return new HdrSnapshot(
                _intervalHistogram,
                _minValue.GetValue(),
                _minUserValue,
                _maxValue.GetValue(),
                _maxUserValue);
        }

        /// <inheritdoc cref="IReservoir" />
        public IReservoirSnapshot GetSnapshot(bool resetReservoir)
        {
            RefreshIfNeeded();
            var snapshot = _cachedSnapshot;

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
            SetMinValue(long.MaxValue, null);
            SetMaxValue(0, null);

            lock (_refreshLock)
            {
                UnsafeRefresh(DateTime.UtcNow.Ticks, true);
            }
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
    }
}