using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tests.TestAttributes;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TimestampTests
    {
        [Test]
        public void AtomicMonotonicTimestampGenerator_Should_Increasing_Monotonic_Values()
        {
            TimestampGeneratorMonitonicityTest(new AtomicMonotonicTimestampGenerator());
        }

        [Test]
        public void AtomicMonotonicTimestampGenerator_Should_Log_When_Drifting_Above_Threshold()
        {
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicTimestampGenerator(80, 1000, new Logger(loggerHandler));
            TimestampGeneratorLogDriftingTest(generator, loggerHandler);
        }

        [Test]
        public void AtomicMonotonicTimestampGenerator_Should_Log_After_Cooldown()
        {
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicTimestampGenerator(80, 1000, new Logger(loggerHandler));
            TimestampGeneratorLogAfterCooldownTest(generator, loggerHandler);
        }

#if !NETCORE
        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Should_Increasing_Monotonic_Values()
        {
            TimestampGeneratorMonitonicityTest(new AtomicMonotonicWinApiTimestampGenerator());
        }

        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Should_Log_When_Drifting_Above_Threshold()
        {
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicWinApiTimestampGenerator(80, 1000, new Logger(loggerHandler));
            TimestampGeneratorLogDriftingTest(generator, loggerHandler);
        }

        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Should_Log_After_Cooldown()
        {
            var loggerHandler = new TestHelper.TestLoggerHandler();
            var generator = new AtomicMonotonicWinApiTimestampGenerator(80, 1000, new Logger(loggerHandler));
            TimestampGeneratorLogAfterCooldownTest(generator, loggerHandler);
        }

        [Test, WinOnly(6, 2)]
        public void AtomicMonotonicWinApiTimestampGenerator_Value_Should_Be_Close_To_Base_Class()
        {
            // The accuracy of both should be within a 15.6ms range
            var generator1 = new AtomicMonotonicTimestampGenerator();
            var generator2 = new AtomicMonotonicWinApiTimestampGenerator();
            Assert.Less(Math.Abs(generator1.Next() - generator2.Next()), 20000);
        }
#endif

        private static void TimestampGeneratorMonitonicityTest(ITimestampGenerator generator)
        {
            TestHelper.ParallelInvoke(() =>
            {
                var lastValue = 0L;
                for (var i = 0; i < 5000; i++)
                {
                    var value = generator.Next();
                    Assert.Greater(value, lastValue);
                    lastValue = value;
                }
            }, 8);
        }

        private static void TimestampGeneratorLogDriftingTest(ITimestampGenerator generator,
                                                              TestHelper.TestLoggerHandler loggerHandler)
        {
            // A little less than 3 seconds
            // It should generate a warning initially and then next 2 after 1 second each
            var maxElapsed = TimeSpan.FromSeconds(2.8);
            TestHelper.ParallelInvoke(() =>
            {
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                while (stopWatch.Elapsed < maxElapsed)
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        generator.Next();
                    }
                }
            }, 2);
            Assert.AreEqual(3, loggerHandler.DequeueAllMessages().Count(i => i.Item1 == "warning"));
        }

        private static void TimestampGeneratorLogAfterCooldownTest(ITimestampGenerator generator,
                                                                   TestHelper.TestLoggerHandler loggerHandler)
        {
            // It should generate a warning initially and then 1 more
            var maxElapsed = TimeSpan.FromSeconds(1.8);
            Action action = () =>
            {
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                // ReSharper disable once AccessToModifiedClosure
                while (stopWatch.Elapsed < maxElapsed)
                {
                    for (var i = 0; i < 5000; i++)
                    {
                        generator.Next();
                    }
                }
            };
            TestHelper.ParallelInvoke(action, 2);
            Assert.AreEqual(2, loggerHandler.DequeueAllMessages().Count(i => i.Item1 == "warning"));
            // Cooldown: make current time > last generated value
            Thread.Sleep(3000);
            // It should generate a warning initially
            maxElapsed = TimeSpan.FromSeconds(0.8);
            TestHelper.ParallelInvoke(action, 2);
            Assert.AreEqual(1, loggerHandler.DequeueAllMessages().Count(i => i.Item1 == "warning"));
        }
    }
}
