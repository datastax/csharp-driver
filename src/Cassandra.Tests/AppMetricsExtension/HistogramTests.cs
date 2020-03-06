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

#if !NET452
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using App.Metrics.ReservoirSampling;

using Cassandra.AppMetrics.Implementations;

using NUnit.Framework;

namespace Cassandra.Tests.AppMetricsExtension
{
    [TestFixture]
    public class HistogramTests
    {
        [Test]
        public void Should_RefreshEntireHistogramAfterRefreshInterval()
        {
            var target = new HdrHistogramReservoir(1, 30000, 3, 1000);

            Parallel.ForEach(Enumerable.Range(1, 100), i => target.Update(i));

            AssertEmptySnapshot(target.GetSnapshot(false));
            AssertEmptySnapshot(target.GetSnapshot(false));

            TestHelper.RetryAssert(
                () =>
                {
                    AssertFirstSnapshot(target.GetSnapshot(false));
                },
                200,
                15);

            Parallel.ForEach(Enumerable.Range(201, 100), i => target.Update(i));

            AssertFirstSnapshot(target.GetSnapshot(false));
            AssertFirstSnapshot(target.GetSnapshot(false));

            TestHelper.RetryAssert(
                () =>
                {
                    AssertSecondSnapshot(target.GetSnapshot(false));
                },
                200,
                15);

            AssertSecondSnapshot(target.GetSnapshot(false));
            AssertSecondSnapshot(target.GetSnapshot(false));
        }

        [Test]
        public async Task Should_ReturnEmptyHistogram_When_ResetIsCalled()
        {
            var target = new HdrHistogramReservoir(1, 1000000000000, 3, 2000);

            var tcs = new CancellationTokenSource();

            AssertEmptySnapshot(target.GetSnapshot(false));

            Parallel.ForEach(Enumerable.Range(1, 100), i => target.Update(i));

            AssertEmptySnapshot(target.GetSnapshot(false));
            await Task.Yield();
            AssertEmptySnapshot(target.GetSnapshot(false));

            TestHelper.RetryAssert(
                () =>
                {
                    AssertFirstSnapshot(target.GetSnapshot(false));
                },
                200,
                20);

            var tasks = Enumerable.Range(0, 4).Select(i => Task.Run(async () =>
            {
                var current = i * 100000;
                while (!tcs.IsCancellationRequested)
                {
                    if (current == int.MaxValue)
                    {
                        current = 0;
                    }

                    target.Update(++current);
                    await Task.Yield();
                }
            })).ToList();

            try
            {
                AssertFirstSnapshot(target.GetSnapshot(false));
                await Task.Yield();
                AssertFirstSnapshot(target.GetSnapshot(false));

                // reset
                AssertFirstSnapshot(target.GetSnapshot(true));

                AssertEmptySnapshot(target.GetSnapshot(false));
                await Task.Yield();
                AssertEmptySnapshot(target.GetSnapshot(false));
            }
            finally
            {
                tcs.Cancel();
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }
        
        [Test]
        public void Should_EmptySnapshotBeThreadSafe()
        {
            var target = new HdrHistogramReservoir(1, 1000, 3, 10000);
            Parallel.ForEach(Enumerable.Range(1, 100000), i => AssertEmptySnapshot(target.GetSnapshot(false)));
            target.Reset();
            Parallel.ForEach(Enumerable.Range(1, 100000), i => AssertEmptySnapshot(target.GetSnapshot(false)));
            Parallel.ForEach(Enumerable.Range(1, 1000), i => AssertEmptySnapshot(target.GetSnapshot(true)));
        }

        private void AssertEmptySnapshot(IReservoirSnapshot snapshot)
        {
            Assert.AreEqual(0d, snapshot.Percentile99);
            Assert.AreEqual(0d, snapshot.Percentile75);
            Assert.AreEqual(0d, snapshot.Percentile98);
            Assert.AreEqual(0d, snapshot.Percentile999);
            Assert.AreEqual(0d, snapshot.Percentile95);
            Assert.AreEqual(0d, snapshot.Max);
            Assert.AreEqual(0d, snapshot.Min);
            Assert.AreEqual(0d, snapshot.Mean);
            Assert.AreEqual(0d, snapshot.Median);
            Assert.AreEqual(0d, snapshot.Count);
        }

        private void AssertFirstSnapshot(IReservoirSnapshot snapshot)
        {
            Assert.AreEqual(99d, snapshot.Percentile99);
            Assert.AreEqual(75d, snapshot.Percentile75);
            Assert.AreEqual(98d, snapshot.Percentile98);
            Assert.AreEqual(100d, snapshot.Percentile999);
            Assert.AreEqual(95d, snapshot.Percentile95);
            Assert.AreEqual(100d, snapshot.Max);
            Assert.AreEqual(1d, snapshot.Min);
            Assert.AreEqual(50.5d, snapshot.Mean);
            Assert.AreEqual(50d, snapshot.Median);
            Assert.AreEqual(100d, snapshot.Count);
        }

        private void AssertSecondSnapshot(IReservoirSnapshot snapshot)
        {
            Assert.AreEqual(299d, snapshot.Percentile99);
            Assert.AreEqual(275d, snapshot.Percentile75);
            Assert.AreEqual(298d, snapshot.Percentile98);
            Assert.AreEqual(300d, snapshot.Percentile999);
            Assert.AreEqual(295d, snapshot.Percentile95);
            Assert.AreEqual(300d, snapshot.Max);
            Assert.AreEqual(201d, snapshot.Min);
            Assert.AreEqual(250.5d, snapshot.Mean);
            Assert.AreEqual(250d, snapshot.Median);
            Assert.AreEqual(100d, snapshot.Count);
        }
    }
}
#endif