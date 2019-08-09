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
using Cassandra.Metrics.DriverAbstractions;
using Cassandra.Metrics.NoopImpl;
using Cassandra.Observers.Abstractions;
using Cassandra.Responses;

namespace Cassandra.Observers
{
    internal class OperationObserver : IOperationObserver
    {
        private readonly IDriverTimer _operationTimer = EmptyDriverTimer.Instance;
        private IDriverTimeHandler _driverTimeHandler = EmptyDriverTimeHandler.Instance;

        public OperationObserver()
        {
        }

        public OperationObserver(IDriverTimer operationTimer)
        {
            _operationTimer = operationTimer;
        }

        public void OnOperationSend(long requestSize)
        {
            _driverTimeHandler = _operationTimer.StartRecording();
        }

        public void OnOperationReceive(Exception exception, Response response)
        {
            // todo(sivukhin, 08.08.2019): Handle exception here?
            _driverTimeHandler.EndRecording();
        }
    }
}