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
    internal class SingleThreadedResolver
    {
        private volatile Task _currentTask;

        public SemaphoreSlim RefreshSemaphoreSlim { get; } = new SemaphoreSlim(1, 1);

        /// <summary>
        /// This method makes sure that there are no concurrent refresh operations.
        /// </summary>
        public async Task RefreshIfNeededAsync(Func<bool> refreshNeeded, Func<Task> refreshFunc)
        {
            var task = _currentTask;
            if (!refreshNeeded() && task != null && !task.IsFaulted)
            {
                await task.ConfigureAwait(false);
                return;
            }

            if (task != null && !task.HasFinished())
            {
                await task.ConfigureAwait(false);
                return;
            }

            // Use a lock for avoiding concurrent calls to RefreshAsync()
            await RefreshSemaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                task = _currentTask;
                if (task == null || (task.HasFinished() && refreshNeeded()) || task.IsFaulted)
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