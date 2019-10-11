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
using Cassandra.Connections;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;
using Cassandra.Responses;

namespace Cassandra.Observers
{
    internal class MetricsOperationObserver : IOperationObserver
    {
        private static readonly Logger Logger = new Logger(typeof(MetricsOperationObserver));

        private readonly IDriverTimer _operationTimer;
        private IDriverTimerMeasurement _driverTimerMeasurement;

        public MetricsOperationObserver(INodeMetrics nodeMetrics)
        {
            _operationTimer = nodeMetrics.CqlMessages;
        }

        public void OnOperationSend(long requestSize, long timestamp)
        {
            try
            {
                _driverTimerMeasurement = _operationTimer.StartMeasuring(timestamp);
            }
            catch (Exception ex)
            {
                MetricsOperationObserver.LogError(ex);
                _driverTimerMeasurement = null;
            }
        }

        public void OnOperationReceive(IRequestError error, Response response, long timestamp)
        {
            try
            {
                if (_driverTimerMeasurement == null)
                {
                    MetricsOperationObserver.Logger.Warning("Found null measurement");
                    return;
                }

                _driverTimerMeasurement.StopMeasuring(timestamp);
                _driverTimerMeasurement = null;
            }
            catch (Exception ex)
            {
                MetricsOperationObserver.LogError(ex);
                _driverTimerMeasurement = null;
            }
        }

        private static void LogError(Exception ex)
        {
            MetricsOperationObserver.Logger.Warning("An error occured while recording metrics for a connection operation. Exception = {0}", ex.ToString());
        }
    }
}