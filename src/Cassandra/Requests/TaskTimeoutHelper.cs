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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.Requests
{
    internal class TaskTimeoutHelper<T> : IDisposable
    {
        private readonly CancellationTokenSource _tcs;
        private readonly Task _timeoutTask;

        public TaskTimeoutHelper(Task<T> taskToWait, TimeSpan timeout)
        {
            TaskToWait = taskToWait;
            _tcs = new CancellationTokenSource();
            _timeoutTask = Task.Delay(timeout, _tcs.Token);
        }
        
        public TaskTimeoutHelper(IEnumerable<Task<T>> tasksToWait, TimeSpan timeout)
        {
            foreach (var task in tasksToWait)
            {
                TaskToWait = TaskToWait == null 
                    ? task 
                    : TaskToWait.ContinueWith(
                        prevTask => prevTask.IsFaulted 
                            ? prevTask 
                            : task, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
            }
            _tcs = new CancellationTokenSource();
            _timeoutTask = Task.Delay(timeout, _tcs.Token);
        }

        public Task<T> TaskToWait { get; }

        public bool WaitWithTimeout()
        { 
            return Task.WaitAny(TaskToWait, _timeoutTask) == 0;
        }

        public async Task<bool> WaitWithTimeoutAsync()
        {
            return (await Task.WhenAny(TaskToWait, _timeoutTask).ConfigureAwait(false)) == TaskToWait;
        }

        public void Dispose()
        {
            _tcs.Dispose();
        }
    }
}