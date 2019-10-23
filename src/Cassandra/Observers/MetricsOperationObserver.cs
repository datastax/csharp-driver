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
using System.Diagnostics;
using System.Threading;
using Cassandra.Connections;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;
using Cassandra.Responses;

namespace Cassandra.Observers
{
    internal class MetricsOperationObserver : IOperationObserver
    {
        private readonly bool _enabledNodeTimerMetrics;
        private static readonly Logger Logger = new Logger(typeof(MetricsOperationObserver));
        private static readonly long Factor = 1000L * 1000L * 1000L / Stopwatch.Frequency;

        private readonly IDriverTimer _operationTimer;
        private long _startTimestamp;

        public MetricsOperationObserver(INodeMetrics nodeMetrics, bool enabledNodeTimerMetrics)
        {
            _enabledNodeTimerMetrics = enabledNodeTimerMetrics;
            _operationTimer = nodeMetrics.CqlMessages;
        }

        public void OnOperationSend(long requestSize, long timestamp)
        {
            if (!_enabledNodeTimerMetrics)
            {
                return;
            }

            try
            {
                Volatile.Write(ref _startTimestamp, timestamp);
            }
            catch (Exception ex)
            {
                MetricsOperationObserver.LogError(ex);
            }
        }

        public void OnOperationReceive(IRequestError error, Response response, long timestamp)
        {
            if (!_enabledNodeTimerMetrics)
            {
                return;
            }

            try
            {
                var startTimestamp = Volatile.Read(ref _startTimestamp);
                if (startTimestamp == 0)
                {
                    MetricsOperationObserver.Logger.Warning("Start timestamp wasn't recorded, discarding this measurement.");
                    return;
                }

                _operationTimer.Record((timestamp - startTimestamp) * MetricsOperationObserver.Factor);
            }
            catch (Exception ex)
            {
                MetricsOperationObserver.LogError(ex);
            }
        }

        private static void LogError(Exception ex)
        {
            MetricsOperationObserver.Logger.Warning("An error occured while recording metrics for a connection operation. Exception = {0}", ex.ToString());
        }
    }
}