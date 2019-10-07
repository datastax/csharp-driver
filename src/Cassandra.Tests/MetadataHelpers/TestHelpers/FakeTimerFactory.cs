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
//

using System.Collections.Concurrent;
using System.Threading.Tasks;

using Cassandra.ProtocolEvents;

using Moq;

namespace Cassandra.Tests.MetadataHelpers.TestHelpers
{
    internal class FakeTimerFactory : ITimerFactory
    {
        public ConcurrentQueue<ITimer> CreatedTimers { get; } = new ConcurrentQueue<ITimer>();

        public ITimer Create(TaskScheduler scheduler)
        {
            var timer = Mock.Of<ITimer>();
            CreatedTimers.Enqueue(timer);
            return timer;
        }
    }
}