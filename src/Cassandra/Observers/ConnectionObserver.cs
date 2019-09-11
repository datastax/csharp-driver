// 
//       Copyright (C) 2019 DataStax Inc.
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
// 

using System;
using System.Net.Sockets;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    // todo(sivukhin, 09.08.2019): Warn about calls to the empty metrics?
    internal class ConnectionObserver : IConnectionObserver
    {
        private readonly Logger _logger = new Logger(typeof(ConnectionObserver));
        private readonly SessionObserver _sessionObserver = new SessionObserver();
        private readonly HostObserver _hostObserver = new HostObserver();

        public ConnectionObserver()
        {
        }

        public ConnectionObserver(SessionObserver sessionObserver, HostObserver hostObserver)
        {
            _sessionObserver = sessionObserver;
            _hostObserver = hostObserver;
        }

        public void SendBytes(long size)
        {
            _logger.Info($"Send {size} bytes");
            _sessionObserver.SessionLevelMetricsRegistry.BytesSent.Increment(size);
            _hostObserver.NodeLevelMetricsRegistry.BytesSent.Increment(size);
        }

        public void ReceiveBytes(long size)
        {
            _logger.Info($"Received {size} bytes");
            _sessionObserver.SessionLevelMetricsRegistry.BytesReceived.Increment(size);
            _hostObserver.NodeLevelMetricsRegistry.BytesReceived.Increment(size);
        }

        public void OnErrorOnOpen(Exception exception)
        {
            switch (exception)
            {
                case AuthenticationException _:
                    _hostObserver.NodeLevelMetricsRegistry.AuthenticationErrors.Increment(1);
                    break;
                case Exception e when e is SocketException || e is UnsupportedProtocolVersionException:
                    _hostObserver.NodeLevelMetricsRegistry.ConnectionInitErrors.Increment(1);
                    break;
            }
        }

        public IOperationObserver CreateOperationObserver()
        {
            return new OperationObserver(_hostObserver.NodeLevelMetricsRegistry.CqlMessages);
        }
    }
}