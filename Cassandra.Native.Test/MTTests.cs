using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Net;

namespace Cassandra.Native.Test
{
    public class MTTests : IUseFixture<Dev.SettingsFixture>, IDisposable
    {
        CassandraManager manager;

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            var serverSp = setFix.Settings["CassandraServer"].Split(':');

            string ip = serverSp[0];
            int port = int.Parse(serverSp[1]);

            var serverAddress = new IPEndPoint(IPAddress.Parse(ip), port);

            manager = new CassandraManager(new List<IPEndPoint>() { serverAddress });
        }

        public void Dispose()
        {
            manager.Dispose();
        }

        public void ConnectionsTest(bool sync)
        {
            int cnt = 10;

            for (int j = 0; j < cnt; j++)
            {
                var conns = new CassandraManagedConnection[cnt];
                try
                {
                    for (int i = 0; i < cnt; i++)
                        conns[i] = manager.Connect();

                    for (int i = 0; i < cnt; i++)
                        conns[i].ExecuteQuery("USE unknknk");
                }
                finally
                {
                    for (int i = 0; i < cnt; i++)
                        if (conns[i] != null)
                            conns[i].Dispose();
                }
            }
        }

        [Fact]
        public void ConnectionsTestBufferedCompressed()
        {
            ConnectionsTest(true);
        }

        [Fact]
        public void ConnectionsTestNoBuffering()
        {
            ConnectionsTest(false);
        }
    }
}
