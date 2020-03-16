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
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Tasks;

namespace Cassandra.Connections
{
    internal abstract class SingleThreadedResolver
    {
        private volatile Task _currentTask;

        protected SemaphoreSlim RefreshSemaphoreSlim { get; } = new SemaphoreSlim(1, 1);

        /// <summary>
        /// This method makes sure that there are no concurrent refresh operations.
        /// </summary>
        protected async Task SafeRefreshIfNeededAsync(Func<bool> refreshNeeded, Func<Task> refreshFunc)
        {
            if (!refreshNeeded())
            {
                return;
            }

            var task = _currentTask;
            if (task != null && !task.HasFinished())
            {
                await task.ConfigureAwait(false);
                return;
            }

            // Use a lock for avoiding concurrent calls to RefreshAsync()
            await RefreshSemaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!refreshNeeded())
                {
                    return;
                }

                var newTask = _currentTask;

                if ((newTask == null && task != null)
                    || (newTask != null && task == null)
                    || (newTask != null && task != null && !object.ReferenceEquals(newTask, task)))
                {
                    // another thread refreshed
                    task = newTask ?? TaskHelper.Completed;
                }
                else
                {
                    task = refreshFunc();
                    _currentTask = task;
                }
            }
            finally
            {
                RefreshSemaphoreSlim.Release();
            }

            await task.ConfigureAwait(false);
        }
    }
}