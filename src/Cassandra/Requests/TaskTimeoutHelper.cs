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
    internal class TaskTimeoutHelper<T>
    {
        public TaskTimeoutHelper(Task<T> taskToWait)
        {
            TaskToWait = taskToWait;
        }
        
        public TaskTimeoutHelper(IEnumerable<Task<T>> tasksToWait)
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
        }

        public Task<T> TaskToWait { get; }

        public bool WaitWithTimeout(TimeSpan timeout)
        {
            using (var tcs = new CancellationTokenSource())
            {
                var timeoutTask = Task.Delay(timeout, tcs.Token);

                var finishedTask = Task.WaitAny(TaskToWait, timeoutTask);

                tcs.Cancel();
                try
                {
                    timeoutTask.GetAwaiter().GetResult();
                }
                catch
                {
                }

                return finishedTask == 0;
            }
        }

        public async Task<bool> WaitWithTimeoutAsync(TimeSpan timeout)
        {
            using (var tcs = new CancellationTokenSource())
            {
                var timeoutTask = Task.Delay(timeout, tcs.Token);

                var finishedTask = await Task.WhenAny(TaskToWait, timeoutTask).ConfigureAwait(false);

                tcs.Cancel();
                try
                {
                    await timeoutTask.ConfigureAwait(false);
                }
                catch
                {
                }

                return finishedTask == TaskToWait;
            }
        }
    }
}