using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture]
    public class ConnectionTests
    {
        [Test]
        public void StartupTest()
        {
            var connection = new Connection(new IPEndPoint(new IPAddress(new byte[]{127, 0, 0, 1}), 9042), new ProtocolOptions(), new SocketOptions());
            connection.Init();
            var task = connection.Startup();
            task.Wait();
        }
    }
}
