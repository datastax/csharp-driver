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

namespace Cassandra.Observers
{
    internal class RequestObserver : IRequestObserver
    {
        private readonly IDriverTimer _requestTimer = EmptyDriverTimer.Instance;
        private IDriverTimeHandler _driverTimeHandler = EmptyDriverTimeHandler.Instance;

        public RequestObserver()
        {
        }

        public RequestObserver(IDriverTimer requestTimer)
        {
            _requestTimer = requestTimer;
        }

        public void OnRequestStart()
        {
            _driverTimeHandler = _requestTimer.StartRecording();
        }

        public void OnRequestFinish(Exception exception)
        {
            _driverTimeHandler.EndRecording();
        }
    }
}