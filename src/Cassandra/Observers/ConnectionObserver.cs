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
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    internal class ConnectionObserver : IConnectionObserver
    {
        private static readonly Logger Logger = new Logger(typeof(ConnectionObserver));
        private readonly ISessionMetrics _sessionMetrics;
        private readonly INodeMetrics _nodeMetrics;

        public ConnectionObserver(ISessionMetrics sessionMetrics, INodeMetrics nodeMetrics)
        {
            _sessionMetrics = sessionMetrics;
            _nodeMetrics = nodeMetrics;
        }

        public void SendBytes(long size)
        {
            try
            {
                _nodeMetrics.BytesSent.Increment(size);
                _sessionMetrics.BytesSent.Increment(size);
            }
            catch (Exception ex)
            {
                LogError(ex);
            }
        }

        public void ReceiveBytes(long size)
        {
            try
            {
                _nodeMetrics.BytesReceived.Increment(size);
                _sessionMetrics.BytesReceived.Increment(size);
            }
            catch (Exception ex)
            {
                LogError(ex);
            }
        }

        public void OnErrorOnOpen(Exception exception)
        {
            try
            {
                switch (exception)
                {
                    case AuthenticationException _:
                        _nodeMetrics.AuthenticationErrors.Increment(1);
                        break;
                    case Exception e when e is SocketException || e is UnsupportedProtocolVersionException:
                        _nodeMetrics.ConnectionInitErrors.Increment(1);
                        break;
                }
            }
            catch (Exception ex)
            {
                LogError(ex);
            }
        }
        
        public IOperationObserver CreateOperationObserver()
        {
            return new OperationObserver(_nodeMetrics);
        }

        private static void LogError(Exception ex)
        {
            Logger.Warning("An error occured while recording metrics for a connection. Exception: {0}", ex.ToString());
        }
    }
}