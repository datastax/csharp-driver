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

using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers
{
    internal class MetricsConnectionObserver : IConnectionObserver
    {
        private static readonly Logger Logger = new Logger(typeof(MetricsConnectionObserver));
        private readonly ISessionMetrics _sessionMetrics;
        private readonly INodeMetrics _nodeMetrics;
        private readonly bool _enabledNodeTimerMetrics;

        public MetricsConnectionObserver(ISessionMetrics sessionMetrics, INodeMetrics nodeMetrics, bool enabledNodeTimerMetrics)
        {
            _sessionMetrics = sessionMetrics;
            _nodeMetrics = nodeMetrics;
            _enabledNodeTimerMetrics = enabledNodeTimerMetrics;
        }

        public void OnBytesSent(long size)
        {
            try
            {
                _nodeMetrics.BytesSent.Mark(size);
                _sessionMetrics.BytesSent.Mark(size);
            }
            catch (Exception ex)
            {
                LogError(ex);
            }
        }

        public void OnBytesReceived(long size)
        {
            try
            {
                _nodeMetrics.BytesReceived.Mark(size);
                _sessionMetrics.BytesReceived.Mark(size);
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
                        _nodeMetrics.Errors.AuthenticationErrors.Increment(1);
                        break;

                    default:
                        _nodeMetrics.Errors.ConnectionInitErrors.Increment(1);
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
            return new MetricsOperationObserver(_nodeMetrics, _enabledNodeTimerMetrics);
        }

        private static void LogError(Exception ex)
        {
            Logger.Warning("An error occured while recording metrics for a connection. Exception: {0}", ex.ToString());
        }
    }
}