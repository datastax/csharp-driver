#if NET452 && !LINUX
using System;
using System.Net.Sockets;
using System.Threading;
using Cassandra.Tests;
using JetBrains.dotMemoryUnit;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("memory"), Explicit("this test needs dotMemory")]
    class MemoryLeakTests
    {
        [Test]
        public void Monitor_Should_Not_Leak_Connections_Test()
        {
            var presetQueryCassandraObjects = QueryBuilder.GetObjects(where => where.Namespace.Like("Cassandra*")
                                                    & where.Namespace.NotLike("Cassandra*Tests"));
            var memoryBeforeAll = dotMemory.Check(memory =>
            {
                TestContext.WriteLine("Before cluster creation");
                var cassandraPackage = memory.GetObjects(presetQueryCassandraObjects);
                var socketObjects = memory.GetObjects(where => where.Type.Is<Socket>());
                TestContext.WriteLine("=== Cassandra driver Object count: " + cassandraPackage.ObjectsCount);
                TestContext.WriteLine("=== Number of Socket objects: " + socketObjects.ObjectsCount);
            });

            var socketOptions = new SocketOptions().SetReadTimeoutMillis(1).SetConnectTimeoutMillis(1);
            var builder = Cluster.Builder()
                                    .AddContactPoint(TestHelper.UnreachableHostAddress)
                                    .WithSocketOptions(socketOptions);

            const int length = 1000;
            using (var cluster = builder.Build())
            {
                var firstExcepetion = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, firstExcepetion.Errors.Count);
                TestContext.WriteLine("After first attempt");
                var memoryBeforeAttempts = dotMemory.Check(memory =>
                {
                    var cassandraPackage = memory.GetObjects(presetQueryCassandraObjects);
                    var socketObjects = memory.GetObjects(where => where.Type.Is<Socket>());
                    TestContext.WriteLine("=== Cassandra driver Object count: " + cassandraPackage.ObjectsCount);
                    TestContext.WriteLine("=== Number of Socket objects: " + socketObjects.ObjectsCount);
                });

                for (var i = 0; i < length; i++)
                {
                    var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                    Assert.AreEqual(1, ex.Errors.Count);
                }
                GC.Collect();
                dotMemory.Check(memory =>
                {
                    TestContext.WriteLine("After " + length + " more attempts");
                    var cassandraPackage = memory.GetObjects(presetQueryCassandraObjects);
                    var socketObjects = memory.GetObjects(where => where.Type.Is<Socket>());
                    TestContext.WriteLine("=== Cassandra driver Object count: " + cassandraPackage.ObjectsCount);
                    TestContext.WriteLine("=== Number of Socket objects: " + socketObjects.ObjectsCount);
                    var leaked = memory.GetDifference(memoryBeforeAttempts)
                                        .GetSurvivedObjects(o => o.LeakedOnEventHandler())
                                        .GetObjects(presetQueryCassandraObjects).ObjectsCount;
                    TestContext.WriteLine("=== Difference: Leaked: " + leaked);
                    Assert.That(leaked, Is.EqualTo(0));

                    var newObjects = memory.GetDifference(memoryBeforeAttempts)
                                            .GetNewObjects().GetObjects(presetQueryCassandraObjects).ObjectsCount;
                    TestContext.WriteLine("=== Difference: New Objects: " + newObjects);
                    Assert.That(newObjects, Is.LessThanOrEqualTo(1), "Increased the amount of new objects");

                    Assert.That(memory.GetDifference(memoryBeforeAll)
                        .GetNewObjects(o => o.Type.Is<Socket>()).ObjectsCount, Is.LessThanOrEqualTo(1),
                        "Should have clean all socket objects");
                });
            }
            GC.Collect();
            dotMemory.Check(memory =>
            {
                TestContext.WriteLine("After cluster disposal");
                var cassandraPackage = memory.GetObjects(presetQueryCassandraObjects);
                var socketObjects = memory.GetObjects(where => where.Type.Is<Socket>());
                TestContext.WriteLine("=== Cassandra driver Object count: " + cassandraPackage.ObjectsCount);
                TestContext.WriteLine("=== Number of Socket objects: " + socketObjects.ObjectsCount);

                var leaked = memory.GetDifference(memoryBeforeAll)
                                    .GetNewObjects(o => o.LeakedOnEventHandler())
                                    .GetObjects(presetQueryCassandraObjects).ObjectsCount;

                TestContext.WriteLine("=== Difference: Leaked: " + leaked);
                Assert.That(leaked, Is.EqualTo(0));
                var newObjects = memory.GetDifference(memoryBeforeAll)
                                        .GetNewObjects().GetObjects(presetQueryCassandraObjects).ObjectsCount;
                TestContext.WriteLine("=== Difference: New Objects: " + newObjects);
                //Assert.That(newObjects, Is.EqualTo(0), "Should have clean all created objects");
                Assert.That(memory.GetDifference(memoryBeforeAll)
                    .GetSurvivedObjects(o => o.Type.Is<Socket>()).ObjectsCount, Is.LessThanOrEqualTo(4), "");
            });
        }
    }
}
#endif
