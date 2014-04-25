using System;
using System.Diagnostics;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cassandra.MSTest
{
    [TestClass]
    public class ConnectionTimeoutTest
    {
        [TestMethod]
        public void connectionTimeoutTest()
        {
            var sw = Stopwatch.StartNew();
            var thrown = false;
            try
            {
                var builder = new Builder().WithDefaultKeyspace("system")
                                           .AddContactPoints("1.1.1.1") // IP address that drops (not rejects !) the inbound connection
                                           .WithQueryTimeout(500);
                builder.SocketOptions.SetConnectTimeoutMillis(500);
                var cluster = builder.Build();
                cluster.Connect();
            }
            catch (NoHostAvailableException)
            {
                thrown = true;
            }

            sw.Stop();

            Assert.True(thrown);
            Assert.True(sw.Elapsed.TotalMilliseconds < 1000, "The connection timeout was not respected");
        }
    }
}