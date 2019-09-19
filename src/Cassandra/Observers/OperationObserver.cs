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
        private IDriverTimeHandler _driverTimeHandler;

        public OperationObserver(INodeMetrics nodeMetrics)
        {
            _operationTimer = nodeMetrics.CqlMessages;
        }

        public void OnOperationSend(long requestSize)
        {
            try
            {
                _driverTimeHandler = _operationTimer.StartRecording();
            }
            catch (Exception ex)
            {
                LogError(ex);
                _driverTimeHandler = NullDriverTimeHandler.Instance;
            }
        }

        public void OnOperationReceive(Exception exception, Response response)
        {
            try
            {
                _driverTimeHandler.EndRecording();
            }
            catch (Exception ex)
            {
                LogError(ex);
            }
        }

        private static void LogError(Exception ex)
        {
            Logger.Warning("An error occured while recording metrics for a connection operation. Exception = {0}", ex.ToString());
        }
    }
}