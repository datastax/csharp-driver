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

namespace Cassandra.ProtocolEvents
{
    internal class TaskBasedTimer : ITimer
    {
        private static readonly Logger Logger = new Logger(typeof(TaskBasedTimer));

        private volatile Task _task;
        private volatile CancellationTokenSource _cts;

        private readonly TaskFactory _taskFactory;
        private volatile bool _disposed = false;

        public TaskBasedTimer(TaskScheduler scheduler)
        {
            _taskFactory = new TaskFactory(
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                TaskContinuationOptions.DenyChildAttach,
                scheduler);
        }

        public void Dispose()
        {
            // must NOT be running within the exclusive scheduler

            var task = _taskFactory.StartNew(() =>
            {
                _disposed = true;
                Cancel();
                return _task;
            }).GetAwaiter().GetResult(); // wait for Cancel

            task?.GetAwaiter().GetResult(); // wait for _task
        }

        public void Cancel()
        {
            // must be running within the exclusive scheduler

            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;
        }

        public void Change(Action action, TimeSpan due)
        {
            // must be running within the exclusive scheduler

            if (_disposed)
            {
                return;
            }

            Cancel();

            _cts = new CancellationTokenSource();
            var cts = _cts;

            // ReSharper disable once MethodSupportsCancellation
            _task = Task.Run(async () =>
            {
                // not running within exclusive scheduler
                try
                {
                    if (cts.IsCancellationRequested)
                    {
                        return;
                    }

                    await Task.Delay(due, cts.Token).ConfigureAwait(false);

                    // ReSharper disable once MethodSupportsCancellation
                    var t = _taskFactory.StartNew(() =>
                    {
                        // running within exclusive scheduler
                        if (!cts.IsCancellationRequested)
                        {
                            action();
                        }
                    });

                    await t.ConfigureAwait(false);
                }
                catch (TaskCanceledException) { }
                catch (ObjectDisposedException) { }
                catch (Exception ex)
                {
                    TaskBasedTimer.Logger.Error("Exception thrown in TaskBasedTimer.", ex);
                }
            });
        }
    }
}