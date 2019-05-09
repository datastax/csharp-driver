//
//       Copyright (C) DataStax Inc.
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
using System.Threading;

namespace Cassandra.ProtocolEvents
{
    /// <summary>
    /// <para><inheritdoc /></para>
    /// <para>
    /// Wrapper around the .NET System.Threading.Timer that implements our custom <see cref="ITimer"/> interface.
    /// </para>
    /// </summary>
    internal class DotnetTimer : ITimer
    {
        private readonly Timer _timer;

        public DotnetTimer(TimerCallback action, object state, TimeSpan due, TimeSpan period)
        {
            _timer = new Timer(action, state, due, period);
        }

        public void Dispose()
        {
            _timer.Dispose();
        }

        public bool Change(TimeSpan due, TimeSpan period)
        {
            return _timer.Change(due, period);
        }
    }
}