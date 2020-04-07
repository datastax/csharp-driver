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

using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;

namespace Cassandra.Tests.Connections.Control.TestHelpers
{
    internal class FakeConnectionEndPoint : IConnectionEndPoint
    {
        public FakeConnectionEndPoint(string address, int port)
        {
            SocketIpEndPoint = new IPEndPoint(IPAddress.Parse(address), port);
        }

        public bool Equals(IConnectionEndPoint other)
        {
            return SocketIpEndPoint.Equals(other.SocketIpEndPoint);
        }

        public IContactPoint ContactPoint => null;

        public IPEndPoint SocketIpEndPoint { get; private set; }

        public string EndpointFriendlyName => SocketIpEndPoint.ToString();

        public Task<string> GetServerNameAsync()
        {
            return Dns.GetHostEntryAsync(SocketIpEndPoint.Address).ContinueWith(t => t.Result.HostName);
        }

        public IPEndPoint GetHostIpEndPointWithFallback()
        {
            return SocketIpEndPoint;
        }

        public IPEndPoint GetOrParseHostIpEndPoint(IRow row, IAddressTranslator translator, int port)
        {
            return SocketIpEndPoint;
        }
    }
}