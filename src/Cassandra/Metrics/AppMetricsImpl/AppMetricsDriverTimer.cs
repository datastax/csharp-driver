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

#if NETSTANDARD2_0

using App.Metrics.Timer;
using Cassandra.Metrics.DriverAbstractions;

namespace Cassandra.Metrics.AppMetricsImpl
{
    internal class AppMetricsDriverTimer : IDriverTimer
    {
        private readonly ITimer _timer;

        public AppMetricsDriverTimer(ITimer timer)
        {
            _timer = timer;
        }

        public IDriverTimeHandler StartRecording()
        {
            return new AppMetricsDriverTimeHandler(_timer.NewContext());
        }
    }
}
#endif