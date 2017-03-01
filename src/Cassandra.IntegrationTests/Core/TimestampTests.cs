using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class TimestampTests : SharedClusterTest
    {
        public TimestampTests() : base(1, false)
        {
            
        }

        [Test]
        public void Cluster_Uses_Provided_Timestamp_Generator()
        {
            var generator = new TestTimestampGenerator();
            using (var cluster = Cluster.Builder()
                                        .WithTimestampGenerator(generator)
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                TestHelper.ParallelInvoke(() => session.Execute("SELECT * FROM system.local"), 10);
                // The driver should use the generator against C* 2.1+
                Assert.AreEqual(CassandraVersion < new Version(2, 1) ? 0 : 10, generator.GetCounter());
            }
        }

        private class TestTimestampGenerator : ITimestampGenerator
        {
            private int _counter;

            public long GetCounter()
            {
                return Volatile.Read(ref _counter);
            }

            public long Next()
            {
                return Interlocked.Increment(ref _counter);
            }
        }
    }
}
