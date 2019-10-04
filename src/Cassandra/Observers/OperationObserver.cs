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
    internal class OperationObserver : IOperationObserver
    {
        private static readonly Logger Logger = new Logger(typeof(OperationObserver));

        private readonly IDriverTimer _operationTimer;
        private IDriverTimerMeasurement _driverTimerMeasurement;

        public OperationObserver(INodeMetrics nodeMetrics)
        {
            _operationTimer = nodeMetrics.CqlMessages;
        }

        public void OnOperationSend(long requestSize)
        {
            try
            {
                _driverTimerMeasurement = _operationTimer.StartMeasuring();
            }
            catch (Exception ex)
            {
                LogError(ex);
                _driverTimerMeasurement = NullDriverTimerMeasurement.Instance;
            }
        }

        public void OnOperationReceive(IRequestError error, Response response)
        {
            try
            {
                _driverTimerMeasurement.StopMeasuring();
            }
            catch (Exception ex)
            {
                OperationObserver.LogError(ex);
            }
        }

        private static void LogError(Exception ex)
        {
            OperationObserver.Logger.Warning("An error occured while recording metrics for a connection operation. Exception = {0}", ex.ToString());
        }
    }
}