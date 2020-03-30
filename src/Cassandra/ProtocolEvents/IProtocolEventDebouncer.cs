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

using System.Threading.Tasks;

namespace Cassandra.ProtocolEvents
{
    /// <summary>
    /// Component that coalesces protocol events into a single queue.
    /// The public methods allow the client to schedule events for processing. Each scheduled event
    /// will move the sliding window of the internal timer forward until a certain max delay has been hit.
    /// The entire queue will be processed once the timeout has passed without any more events coming in or after
    /// the max delay has passed.
    /// </summary>
    internal interface IProtocolEventDebouncer
    {
        /// <summary>
        /// Returned task will be complete when the event has been scheduled for processing.
        /// </summary>
        Task ScheduleEventAsync(ProtocolEvent ev, bool processNow);

        /// <summary>
        /// Returned task will be complete when the event has been processed.
        /// </summary>
        Task HandleEventAsync(ProtocolEvent ev, bool processNow);

        Task ShutdownAsync();
    }
}