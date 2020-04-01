//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Dse.Test.Unit.TestAttributes;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [TestFixture]
    public class TimestampTests
    {
        [Test]
        public void AtomicMonotonicTimestampGenerator_Next_Should_Return_Increasing_Monotonic_Values()
        {
            TimestampGeneratorMonitonicityTest(new AtomicMonotonicTimestampGenerator());
        }

        [Test]
        public void AtomicMonotonicTimestampGenerator_Next_Should_Return_Log_When_Drifting_Above_Threshold()
        {
            var minLogInterval = 2500;
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicTimestampGenerator(5, minLogInterval, new Logger(loggerHandler));
            TimestampGeneratorLogDriftingTest(generator, loggerHandler, minLogInterval);
        }

        [Test]
        public void AtomicMonotonicTimestampGenerator_Next_Should_Log_After_Cooldown()
        {
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicTimestampGenerator(80, 1000, new Logger(loggerHandler));
            TimestampGeneratorLogAfterCooldownTest(generator, loggerHandler);
        }

#if !NETCORE

        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Next_Should_Return_Increasing_Monotonic_Values()
        {
            TimestampGeneratorMonitonicityTest(new AtomicMonotonicWinApiTimestampGenerator());
        }

        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Next_Should_Log_When_Drifting_Above_Threshold()
        {
            var minLogInterval = 2500;
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicWinApiTimestampGenerator(5, minLogInterval, new Logger(loggerHandler));
            TimestampGeneratorLogDriftingTest(generator, loggerHandler, minLogInterval);
        }

        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Next_Should_Log_After_Cooldown()
        {
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicWinApiTimestampGenerator(80, 1000, new Logger(loggerHandler));
            TimestampGeneratorLogAfterCooldownTest(generator, loggerHandler);
        }

        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Next_Should_Return_A_Value_Close_To_Base_Class()
        {
            // The accuracy of both should be within a 15.6ms range
            var generator1 = new AtomicMonotonicTimestampGenerator();
            var generator2 = new AtomicMonotonicWinApiTimestampGenerator();
            Assert.Less(Math.Abs(generator1.Next() - generator2.Next()), 20000);
        }

#endif

        private static void TimestampGeneratorMonitonicityTest(ITimestampGenerator generator)
        {
            // Use a set to determine the amount of different values
            const int iterations = 5000;
            const int threads = 8;
            var values = new ConcurrentDictionary<long, bool>();
            TestHelper.ParallelInvoke(() =>
            {
                var lastValue = 0L;
                for (var i = 0; i < iterations; i++)
                {
                    var value = generator.Next();
                    Assert.Greater(value, lastValue);
                    lastValue = value;
                    values.TryAdd(value, true);
                }
            }, threads);
            Assert.AreEqual(iterations * threads, values.Count);
        }

        private static void TimestampGeneratorLogDriftingTest(
            ITimestampGenerator generator, TestHelper.TestLoggerHandler loggerHandler, int logIntervalMs)
        {
            var timestamp = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
            TestHelper.ParallelInvoke(
                () =>
                {
                    generator.Next();
                },
                1000000);

            var elapsed = (DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond) - timestamp;

            if (elapsed > 3000)
            {
                Assert.Ignore("Generated numbers too slowly for this test to work.");
            }
            else
            {
                var count = elapsed / logIntervalMs;

                Assert.That(Interlocked.Read(ref loggerHandler.WarningCount), Is.InRange(count + 1, count + 2));
            }

        }

        private static void TimestampGeneratorLogAfterCooldownTest(ITimestampGenerator generator,
                                                                   TestHelper.TestLoggerHandler loggerHandler)
        {
            // It should generate a warning initially and then 1 more
            var counter = 0;

            void Action(TimeSpan maxElapsed)
            {
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                while (stopWatch.Elapsed < maxElapsed)
                {
                    for (var i = 0; i < 5000; i++)
                    {
                        generator.Next();
                        // ReSharper disable once AccessToModifiedClosure
                        Interlocked.Increment(ref counter);
                    }
                }
            }

            TestHelper.ParallelInvoke(() => Action(TimeSpan.FromSeconds(1.8)), 2);
            if (Volatile.Read(ref counter) < 5000000)
            {
                // if during this time, we weren't able to generate a lot of values, don't mind
                Assert.Ignore("It was not able to generate 5M values");
            }

            Assert.That(Interlocked.Read(ref loggerHandler.WarningCount), Is.GreaterThanOrEqualTo(2));

            // Cooldown: make current time > last generated value
            Thread.Sleep(4000);

            // It should generate a warning initially
            TestHelper.ParallelInvoke(() => Action(TimeSpan.FromSeconds(0.8)), 2);
            Assert.That(Interlocked.Read(ref loggerHandler.WarningCount), Is.GreaterThanOrEqualTo(1));
        }
    }
}